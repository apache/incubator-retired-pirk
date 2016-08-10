/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pirk.responder.wideskies.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.pirk.inputformat.hadoop.BaseInputFormat;
import org.apache.pirk.inputformat.hadoop.BytesArrayWritable;
import org.apache.pirk.inputformat.hadoop.InputFormatConst;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaLoader;
import org.apache.pirk.schema.query.QuerySchemaRegistry;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.apache.pirk.utils.FileConst;
import org.apache.pirk.utils.HDFS;
import org.apache.pirk.utils.SystemConfiguration;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for computing the PIR response in MapReduce
 * <p>
 * Each query run consists of three MR jobs:
 * <p>
 * (1) Map: Initialization mapper reads data using an extension of the BaseInputFormat or elasticsearch and, according to the QueryInfo object, extracts the
 * selector from each dataElement according to the QueryType, hashes selector, and outputs {@link <hash(selector), dataElement>}
 * <p>
 * Reduce: Calculates the encrypted row values for each selector and corresponding data element, striping across columns,and outputs each row entry by column
 * position: {@link <colNum, colVal>}
 * <p>
 * (2) Map: Pass through mapper to aggregate by column number
 * <p>
 * Reduce: Input: {@link <colnum, <colVals>>}; multiplies all colVals according to the encryption algorithm and outputs {@link <colNum, colVal>} for each colNum
 * <p>
 * (3) Map: Pass through mapper to move all final columns to one reducer
 * <p>
 * Reduce: Creates the Response object
 * <P>
 * NOTE: If useHDFSExpLookupTable in the QueryInfo object is true, then the expLookupTable for the watchlist must be generated if it does not already exist in
 * hdfs.
 * <p>
 * TODO:
 * <p>
 * -Currently processes one query at time - can change to process multiple queries at the same time (under the same time interval and with same query
 * parameters) - using MultipleOutputs for extensibility to multiple queries per job later...
 * <p>
 * - Could place Query objects in DistributedCache (instead of a direct file based pull in task setup)
 * <p>
 * - Redesign exp lookup table to be smart and fully distributed/partitioned
 */
public class ComputeResponseTool extends Configured implements Tool
{
  private static final Logger logger = LoggerFactory.getLogger(ComputeResponseTool.class);

  private String dataInputFormat = null;
  private String inputFile = null;
  private String outputFile = null;
  private String outputDirExp = null;
  private String outputDirInit = null;
  private String outputDirColumnMult = null;
  private String outputDirFinal = null;
  private String queryInputDir = null;
  private String stopListFile = null;
  private int numReduceTasks = 1;

  private boolean useHDFSLookupTable = false;

  private String esQuery = "none";
  private String esResource = "none";

  String dataSchema = "none";

  private Configuration conf = null;
  private FileSystem fs = null;

  private Query query = null;
  private QueryInfo queryInfo = null;
  private QuerySchema qSchema = null;

  public ComputeResponseTool() throws Exception
  {
    setupParameters();

    conf = new Configuration();
    fs = FileSystem.get(conf);

    // Load the schemas
    DataSchemaLoader.initialize(true, fs);
    QuerySchemaLoader.initialize(true, fs);

    query = new HadoopFileSystemStore(fs).recall(queryInputDir, Query.class);
    queryInfo = query.getQueryInfo();
    if (SystemConfiguration.getBooleanProperty("pir.allowAdHocQuerySchemas", false))
    {
      qSchema = queryInfo.getQuerySchema();
    }
    if (qSchema == null)
    {
      qSchema = QuerySchemaRegistry.get(queryInfo.getQueryType());
    }

    logger.info("outputFile = " + outputFile + " outputDirInit = " + outputDirInit + " outputDirColumnMult = " + outputDirColumnMult + " queryInputDir = "
        + queryInputDir + " stopListFile = " + stopListFile + " numReduceTasks = " + numReduceTasks + " esQuery = " + esQuery + " esResource = " + esResource);
  }

  @Override
  public int run(String[] arg0) throws Exception
  {
    boolean success = true;

    Path outPathInit = new Path(outputDirInit);
    Path outPathColumnMult = new Path(outputDirColumnMult);
    Path outPathFinal = new Path(outputDirFinal);

    // If we are using distributed exp tables -- Create the expTable file in hdfs for this query, if it doesn't exist
    if ((queryInfo.getUseHDFSExpLookupTable() || useHDFSLookupTable) && query.getExpFileBasedLookup().isEmpty())
    {
      success = computeExpTable();
    }

    // Read the data, hash selectors, form encrypted rows
    if (success)
    {
      success = readDataEncRows(outPathInit);
    }

    // Multiply the column values
    if (success)
    {
      success = multiplyColumns(outPathInit, outPathColumnMult);
    }

    // Concatenate the output to one file
    if (success)
    {
      success = computeFinalResponse(outPathFinal);
    }

    // Clean up
    fs.delete(outPathInit, true);
    fs.delete(outPathColumnMult, true);
    fs.delete(outPathFinal, true);

    return success ? 0 : 1;
  }

  private void setupParameters()
  {
    dataInputFormat = SystemConfiguration.getProperty("pir.dataInputFormat");
    if (!InputFormatConst.ALLOWED_FORMATS.contains(dataInputFormat))
    {
      throw new IllegalArgumentException("inputFormat = " + dataInputFormat + " is of an unknown form");
    }
    logger.info("inputFormat = " + dataInputFormat);
    if (dataInputFormat.equals(InputFormatConst.BASE_FORMAT))
    {
      inputFile = SystemConfiguration.getProperty("pir.inputData", "none");
      if (inputFile.equals("none"))
      {
        throw new IllegalArgumentException("For inputFormat = " + dataInputFormat + " an inputFile must be specified");
      }
      logger.info("inputFile = " + inputFile);
    }
    else if (dataInputFormat.equals(InputFormatConst.ES))
    {
      esQuery = SystemConfiguration.getProperty("pir.esQuery", "none");
      esResource = SystemConfiguration.getProperty("pir.esResource", "none");
      if (esQuery.equals("none"))
      {
        throw new IllegalArgumentException("esQuery must be specified");
      }
      if (esResource.equals("none"))
      {
        throw new IllegalArgumentException("esResource must be specified");
      }
      logger.info("esQuery = " + esQuery + " esResource = " + esResource);
    }

    outputFile = SystemConfiguration.getProperty("pir.outputFile");
    outputDirInit = outputFile + "_init";
    outputDirExp = outputFile + "_exp";
    outputDirColumnMult = outputFile + "_colMult";
    outputDirFinal = outputFile + "_final";
    queryInputDir = SystemConfiguration.getProperty("pir.queryInput");
    stopListFile = SystemConfiguration.getProperty("pir.stopListFile");

    useHDFSLookupTable = SystemConfiguration.isSetTrue("pir.useHDFSLookupTable");

    numReduceTasks = SystemConfiguration.getIntProperty("pir.numReduceTasks", 1);
  }

  private boolean computeExpTable() throws IOException, ClassNotFoundException, InterruptedException
  {
    boolean success;

    logger.info("Creating expTable");

    // The split location for the interim calculations, delete upon completion
    Path splitDir = new Path("/tmp/splits-" + queryInfo.getQueryNum());
    if (fs.exists(splitDir))
    {
      fs.delete(splitDir, true);
    }
    // Write the query hashes to the split files
    TreeMap<Integer,BigInteger> queryElements = query.getQueryElements();
    ArrayList<Integer> keys = new ArrayList<>(queryElements.keySet());

    int numSplits = SystemConfiguration.getIntProperty("pir.expCreationSplits", 100);
    int elementsPerSplit = (int) Math.floor(queryElements.size() / numSplits);
    logger.info("numSplits = " + numSplits + " elementsPerSplit = " + elementsPerSplit);
    for (int i = 0; i < numSplits; ++i)
    {
      // Grab the range of the thread
      int start = i * elementsPerSplit;
      int stop = start + elementsPerSplit - 1;
      if (i == (numSplits - 1))
      {
        stop = queryElements.size() - 1;
      }
      HDFS.writeFileIntegers(keys.subList(start, stop), fs, new Path(splitDir, "split-" + i), false);
    }

    // Run the job to generate the expTable
    // Job jobExp = new Job(mrConfig.getConfig(), "pirExp-" + pirWL.getWatchlistNum());
    Job jobExp = new Job(conf, "pirExp-" + queryInfo.getQueryNum());

    jobExp.setSpeculativeExecution(false);
    jobExp.getConfiguration().set("mapreduce.map.speculative", "false");
    jobExp.getConfiguration().set("mapreduce.reduce.speculative", "false");

    // Set the memory and heap options
    jobExp.getConfiguration().set("mapreduce.map.memory.mb", SystemConfiguration.getProperty("mapreduce.map.memory.mb", "10000"));
    jobExp.getConfiguration().set("mapreduce.reduce.memory.mb", SystemConfiguration.getProperty("mapreduce.reduce.memory.mb", "10000"));
    jobExp.getConfiguration().set("mapreduce.map.java.opts", SystemConfiguration.getProperty("mapreduce.map.java.opts", "-Xmx9000m"));
    jobExp.getConfiguration().set("mapreduce.reduce.java.opts", SystemConfiguration.getProperty("mapreduce.reduce.java.opts", "-Xmx9000m"));
    jobExp.getConfiguration().set("mapreduce.reduce.shuffle.parallelcopies", "5");

    jobExp.getConfiguration().set("pirMR.queryInputDir", SystemConfiguration.getProperty("pir.queryInput"));
    jobExp.getConfiguration().setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

    jobExp.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.setInputPaths(jobExp, splitDir);

    jobExp.setJarByClass(ExpTableMapper.class);
    jobExp.setMapperClass(ExpTableMapper.class);

    jobExp.setMapOutputKeyClass(Text.class);
    jobExp.setMapOutputValueClass(Text.class);

    // Set the reducer and output params
    int numExpLookupPartitions = SystemConfiguration.getIntProperty("pir.numExpLookupPartitions", 100);
    jobExp.setNumReduceTasks(numExpLookupPartitions);
    jobExp.setReducerClass(ExpTableReducer.class);

    // Delete the output directory if it exists
    Path outPathExp = new Path(outputDirExp);
    if (fs.exists(outPathExp))
    {
      fs.delete(outPathExp, true);
    }
    jobExp.setOutputKeyClass(Text.class);
    jobExp.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(jobExp, outPathExp);
    jobExp.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
    MultipleOutputs.addNamedOutput(jobExp, FileConst.PIR, TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(jobExp, FileConst.EXP, TextOutputFormat.class, Text.class, Text.class);

    // Submit job, wait for completion
    success = jobExp.waitForCompletion(true);

    // Assemble the exp table from the output
    // element_index -> fileName
    HashMap<Integer,String> expFileTable = new HashMap<>();
    FileStatus[] status = fs.listStatus(outPathExp);
    for (FileStatus fstat : status)
    {
      if (fstat.getPath().getName().startsWith(FileConst.PIR))
      {
        logger.info("fstat.getPath().getName().toString() = " + fstat.getPath().getName());
        try
        {
          InputStreamReader isr = new InputStreamReader(fs.open(fstat.getPath()));
          BufferedReader br = new BufferedReader(isr);
          String line;
          while ((line = br.readLine()) != null)
          {
            String[] rowValTokens = line.split(","); // form is element_index,reducerNumber
            String fileName = fstat.getPath().getParent() + "/" + FileConst.EXP + "-r-" + rowValTokens[1];
            logger.info("fileName = " + fileName);
            expFileTable.put(Integer.parseInt(rowValTokens[0]), fileName);
          }

        } catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    }

    // Place exp table in query object
    query.setExpFileBasedLookup(expFileTable);
    new HadoopFileSystemStore(fs).store(queryInputDir, query);

    logger.info("Completed creation of expTable");

    return success;
  }

  private boolean readDataEncRows(Path outPathInit) throws Exception
  {
    boolean success;

    Job job = new Job(conf, "pirMR");
    job.setSpeculativeExecution(false);

    // Set the data and query schema properties
    job.getConfiguration().set("dataSchemaName", qSchema.getDataSchemaName());
    job.getConfiguration().set("data.schemas", SystemConfiguration.getProperty("data.schemas"));
    job.getConfiguration().set("query.schemas", SystemConfiguration.getProperty("query.schemas"));

    // Set the memory and heap options
    job.getConfiguration().set("mapreduce.map.memory.mb", SystemConfiguration.getProperty("mapreduce.map.memory.mb", "2000"));
    job.getConfiguration().set("mapreduce.reduce.memory.mb", SystemConfiguration.getProperty("mapreduce.reduce.memory.mb", "2000"));
    job.getConfiguration().set("mapreduce.map.java.opts", SystemConfiguration.getProperty("mapreduce.map.java.opts", "-Xmx1800m"));
    job.getConfiguration().set("mapreduce.reduce.java.opts", SystemConfiguration.getProperty("mapreduce.reduce.java.opts", "-Xmx1800m"));

    // Set necessary files for Mapper setup
    job.getConfiguration().set("pirMR.queryInputDir", SystemConfiguration.getProperty("pir.queryInput"));
    job.getConfiguration().set("pirMR.stopListFile", SystemConfiguration.getProperty("pir.stopListFile"));

    job.getConfiguration().set("mapreduce.map.speculative", "false");
    job.getConfiguration().set("mapreduce.reduce.speculative", "false");

    job.getConfiguration().set("pirWL.useLocalCache", SystemConfiguration.getProperty("pir.useLocalCache", "true"));
    job.getConfiguration().set("pirWL.limitHitsPerSelector", SystemConfiguration.getProperty("pir.limitHitsPerSelector", "false"));
    job.getConfiguration().set("pirWL.maxHitsPerSelector", SystemConfiguration.getProperty("pir.maxHitsPerSelector", "100"));

    if (dataInputFormat.equals(InputFormatConst.ES))
    {
      String jobName = "pirMR_es_" + esResource + "_" + esQuery + "_" + System.currentTimeMillis();
      job.setJobName(jobName);

      job.getConfiguration().set("es.nodes", SystemConfiguration.getProperty("es.nodes"));
      job.getConfiguration().set("es.port", SystemConfiguration.getProperty("es.port"));
      job.getConfiguration().set("es.resource", esResource);
      job.getConfiguration().set("es.query", esQuery);

      job.setInputFormatClass(EsInputFormat.class);
    }
    else if (dataInputFormat.equals(InputFormatConst.BASE_FORMAT))
    {
      String baseQuery = SystemConfiguration.getProperty("pir.baseQuery");
      String jobName = "pirMR_base_" + baseQuery + "_" + System.currentTimeMillis();
      job.setJobName(jobName);

      job.getConfiguration().set("baseQuery", baseQuery);
      job.getConfiguration().set("query", baseQuery);
      job.getConfiguration().set("pir.allowAdHocQuerySchemas", SystemConfiguration.getProperty("pir.allowAdHocQuerySchemas", "false"));

      job.getConfiguration().setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

      // Set the inputFormatClass based upon the baseInputFormat property
      String classString = SystemConfiguration.getProperty("pir.baseInputFormat");
      Class<BaseInputFormat> inputClass = (Class<BaseInputFormat>) Class.forName(classString);
      if (!Class.forName("org.apache.pirk.inputformat.hadoop.BaseInputFormat").isAssignableFrom(inputClass))
      {
        throw new Exception("baseInputFormat class = " + classString + " does not extend BaseInputFormat");
      }
      job.setInputFormatClass(inputClass);

      FileInputFormat.setInputPaths(job, inputFile);
    }

    job.setJarByClass(HashSelectorsAndPartitionDataMapper.class);
    job.setMapperClass(HashSelectorsAndPartitionDataMapper.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(BytesArrayWritable.class);

    // Set the reducer and output params
    job.setNumReduceTasks(numReduceTasks);
    job.setReducerClass(RowCalcReducer.class);

    // Delete the output directory if it exists
    if (fs.exists(outPathInit))
    {
      fs.delete(outPathInit, true);
    }
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, outPathInit);
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

    MultipleOutputs.addNamedOutput(job, FileConst.PIR, TextOutputFormat.class, LongWritable.class, Text.class);

    // Submit job, wait for completion
    success = job.waitForCompletion(true);

    return success;
  }

  private boolean multiplyColumns(Path outPathInit, Path outPathColumnMult) throws IOException, ClassNotFoundException, InterruptedException
  {
    boolean success;

    Job columnMultJob = new Job(conf, "pir_columnMult");
    columnMultJob.setSpeculativeExecution(false);

    String columnMultJobName = "pir_columnMult";

    // Set the same job configs as for the first iteration
    columnMultJob.getConfiguration().set("mapreduce.map.memory.mb", SystemConfiguration.getProperty("mapreduce.map.memory.mb", "2000"));
    columnMultJob.getConfiguration().set("mapreduce.reduce.memory.mb", SystemConfiguration.getProperty("mapreduce.reduce.memory.mb", "2000"));
    columnMultJob.getConfiguration().set("mapreduce.map.java.opts", SystemConfiguration.getProperty("mapreduce.map.java.opts", "-Xmx1800m"));
    columnMultJob.getConfiguration().set("mapreduce.reduce.java.opts", SystemConfiguration.getProperty("mapreduce.reduce.java.opts", "-Xmx1800m"));

    columnMultJob.getConfiguration().set("mapreduce.map.speculative", "false");
    columnMultJob.getConfiguration().set("mapreduce.reduce.speculative", "false");
    columnMultJob.getConfiguration().set("pirMR.queryInputDir", SystemConfiguration.getProperty("pir.queryInput"));

    columnMultJob.setJobName(columnMultJobName);
    columnMultJob.setJarByClass(ColumnMultMapper.class);
    columnMultJob.setNumReduceTasks(numReduceTasks);

    // Set the Mapper, InputFormat, and input path
    columnMultJob.setMapperClass(ColumnMultMapper.class);
    columnMultJob.setInputFormatClass(TextInputFormat.class);

    FileStatus[] status = fs.listStatus(outPathInit);
    for (FileStatus fstat : status)
    {
      if (fstat.getPath().getName().startsWith(FileConst.PIR))
      {
        logger.info("fstat.getPath() = " + fstat.getPath().toString());
        FileInputFormat.addInputPath(columnMultJob, fstat.getPath());
      }
    }
    columnMultJob.setMapOutputKeyClass(LongWritable.class);
    columnMultJob.setMapOutputValueClass(Text.class);

    // Set the reducer and output options
    columnMultJob.setReducerClass(ColumnMultReducer.class);
    columnMultJob.setOutputKeyClass(LongWritable.class);
    columnMultJob.setOutputValueClass(Text.class);
    columnMultJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

    // Delete the output file, if it exists
    if (fs.exists(outPathColumnMult))
    {
      fs.delete(outPathColumnMult, true);
    }
    FileOutputFormat.setOutputPath(columnMultJob, outPathColumnMult);

    MultipleOutputs.addNamedOutput(columnMultJob, FileConst.PIR_COLS, TextOutputFormat.class, LongWritable.class, Text.class);

    // Submit job, wait for completion
    success = columnMultJob.waitForCompletion(true);

    return success;
  }

  private boolean computeFinalResponse(Path outPathFinal) throws ClassNotFoundException, IOException, InterruptedException
  {
    boolean success;

    Job finalResponseJob = new Job(conf, "pir_finalResponse");
    finalResponseJob.setSpeculativeExecution(false);

    String finalResponseJobName = "pir_finalResponse";

    // Set the same job configs as for the first iteration
    finalResponseJob.getConfiguration().set("mapreduce.map.memory.mb", SystemConfiguration.getProperty("mapreduce.map.memory.mb", "2000"));
    finalResponseJob.getConfiguration().set("mapreduce.reduce.memory.mb", SystemConfiguration.getProperty("mapreduce.reduce.memory.mb", "2000"));
    finalResponseJob.getConfiguration().set("mapreduce.map.java.opts", SystemConfiguration.getProperty("mapreduce.map.java.opts", "-Xmx1800m"));
    finalResponseJob.getConfiguration().set("mapreduce.reduce.java.opts", SystemConfiguration.getProperty("mapreduce.reduce.java.opts", "-Xmx1800m"));

    finalResponseJob.getConfiguration().set("pirMR.queryInputDir", SystemConfiguration.getProperty("pir.queryInput"));
    finalResponseJob.getConfiguration().set("pirMR.outputFile", outputFile);

    finalResponseJob.getConfiguration().set("mapreduce.map.speculative", "false");
    finalResponseJob.getConfiguration().set("mapreduce.reduce.speculative", "false");

    finalResponseJob.setJobName(finalResponseJobName);
    finalResponseJob.setJarByClass(ColumnMultMapper.class);
    finalResponseJob.setNumReduceTasks(1);

    // Set the Mapper, InputFormat, and input path
    finalResponseJob.setMapperClass(ColumnMultMapper.class);
    finalResponseJob.setInputFormatClass(TextInputFormat.class);

    FileStatus[] status = fs.listStatus(new Path(outputDirColumnMult));
    for (FileStatus fstat : status)
    {
      if (fstat.getPath().getName().startsWith(FileConst.PIR_COLS))
      {
        logger.info("fstat.getPath() = " + fstat.getPath().toString());
        FileInputFormat.addInputPath(finalResponseJob, fstat.getPath());
      }
    }
    finalResponseJob.setMapOutputKeyClass(LongWritable.class);
    finalResponseJob.setMapOutputValueClass(Text.class);

    // Set the reducer and output options
    finalResponseJob.setReducerClass(FinalResponseReducer.class);
    finalResponseJob.setOutputKeyClass(LongWritable.class);
    finalResponseJob.setOutputValueClass(Text.class);
    finalResponseJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

    // Delete the output file, if it exists
    if (fs.exists(outPathFinal))
    {
      fs.delete(outPathFinal, true);
    }
    FileOutputFormat.setOutputPath(finalResponseJob, outPathFinal);
    MultipleOutputs.addNamedOutput(finalResponseJob, FileConst.PIR_FINAL, TextOutputFormat.class, LongWritable.class, Text.class);

    // Submit job, wait for completion
    success = finalResponseJob.waitForCompletion(true);

    return success;
  }
}
