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
package org.apache.pirk.responder.wideskies.spark;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pirk.inputformat.hadoop.BaseInputFormat;
import org.apache.pirk.inputformat.hadoop.InputFormatConst;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaLoader;
import org.apache.pirk.schema.query.QuerySchemaRegistry;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Master class for the PIR query spark application
 * <p>
 * NOTE:
 * <p>
 * - NOT using Elasticsearch in practice - proved to be some speed issues with ES and Spark that appear to be ES-Spark specific - leave code in anticipation
 * that the ES-Spark issues resolve...
 * <p>
 * - Even if rdd.count() calls are embedded in logger.debug statements, they are computed by Spark. Thus, they are commented out in the code below - uncomment
 * for rdd.count() debug
 * 
 */
public class ComputeResponse
{
  private static final Logger logger = LoggerFactory.getLogger(ComputeResponse.class);

  private String dataInputFormat = null;
  private String inputData = null;
  private String outputFile = null;
  private String outputDirExp = null;

  private String queryInput = null;

  private String esQuery = "none";
  private String esResource = "none";

  private boolean useHDFSLookupTable = false;
  private boolean useModExpJoin = false;

  private FileSystem fs = null;
  private HadoopFileSystemStore storage = null;
  private JavaSparkContext sc = null;

  private Accumulators accum = null;
  private BroadcastVars bVars = null;

  private QueryInfo queryInfo = null;
  Query query = null;

  private int numDataPartitions = 0;
  private int numColMultPartitions = 0;

  private boolean colMultReduceByKey = false;

  public ComputeResponse(FileSystem fileSys) throws Exception
  {
    fs = fileSys;
    storage = new HadoopFileSystemStore(fs);

    dataInputFormat = SystemConfiguration.getProperty("pir.dataInputFormat");
    if (!InputFormatConst.ALLOWED_FORMATS.contains(dataInputFormat))
    {
      throw new IllegalArgumentException("inputFormat = " + dataInputFormat + " is of an unknown form");
    }
    logger.info("inputFormat = " + dataInputFormat);
    if (dataInputFormat.equals(InputFormatConst.BASE_FORMAT))
    {
      inputData = SystemConfiguration.getProperty("pir.inputData", "none");
      if (inputData.equals("none"))
      {
        throw new IllegalArgumentException("For inputFormat = " + dataInputFormat + " an inputFile must be specified");
      }
      logger.info("inputFile = " + inputData);
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
    outputDirExp = outputFile + "_exp";

    queryInput = SystemConfiguration.getProperty("pir.queryInput");
    String stopListFile = SystemConfiguration.getProperty("pir.stopListFile");
    useModExpJoin = SystemConfiguration.getProperty("pir.useModExpJoin", "false").equals("true");

    logger.info("outputFile = " + outputFile + " queryInputDir = " + queryInput + " stopListFile = " + stopListFile + " esQuery = " + esQuery
        + " esResource = " + esResource);

    // Set the necessary configurations
    SparkConf conf = new SparkConf().setAppName("SparkPIR").setMaster("yarn-cluster");
    conf.set("es.nodes", SystemConfiguration.getProperty("es.nodes", "none"));
    conf.set("es.port", SystemConfiguration.getProperty("es.port", "none"));
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.set("spark.memory.storageFraction", "0.10");
    conf.set("spark.memory.fraction", "0.25");
    // conf.set("spark.memory.fraction", "0.25");
    // conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops");
    sc = new JavaSparkContext(conf);

    // Setup, run query, teardown
    logger.info("Setting up for query run");
    setup();
    logger.info("Setup complete");
  }

  // Setup for the accumulators and broadcast variables
  private void setup() throws Exception
  {
    // Load the schemas
    DataSchemaLoader.initialize(true, fs);
    QuerySchemaLoader.initialize(true, fs);

    // Create the accumulators and broadcast variables
    accum = new Accumulators(sc);
    bVars = new BroadcastVars(sc);

    // Set the Query and QueryInfo broadcast variables
    query = storage.recall(queryInput, Query.class);
    queryInfo = query.getQueryInfo();
    bVars.setQuery(query);
    bVars.setQueryInfo(queryInfo);

    QuerySchema qSchema = null;
    if (SystemConfiguration.getProperty("pir.allowAdHocQuerySchemas", "false").equals("true"))
    {
      qSchema = queryInfo.getQuerySchema();
    }
    if (qSchema == null)
    {
      qSchema = QuerySchemaRegistry.get(queryInfo.getQueryType());
    }

    DataSchema dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());
    bVars.setQuerySchema(qSchema);
    bVars.setDataSchema(dSchema);

    // Set the local cache flag
    bVars.setUseLocalCache(SystemConfiguration.getProperty("pir.useLocalCache", "true"));

    useHDFSLookupTable = SystemConfiguration.getProperty("pir.useHDFSLookupTable").equals("true");

    // Set the hit limit variables
    bVars.setLimitHitsPerSelector(Boolean.valueOf(SystemConfiguration.getProperty("pir.limitHitsPerSelector")));
    bVars.setMaxHitsPerSelector(Integer.parseInt(SystemConfiguration.getProperty("pir.maxHitsPerSelector")));

    // Set the number of data and column multiplication partitions
    String numDataPartsString = SystemConfiguration.getProperty("pir.numDataPartitions", "1000");
    numDataPartitions = Integer.parseInt(numDataPartsString);
    numColMultPartitions = Integer.parseInt(SystemConfiguration.getProperty("pir.numColMultPartitions", numDataPartsString));

    // Whether or not we are performing a reduceByKey or a groupByKey->reduce for column multiplication
    colMultReduceByKey = SystemConfiguration.getProperty("pir.colMultReduceByKey", "false").equals("true");

    // Set the expDir
    bVars.setExpDir(outputDirExp);
  }

  // Method to tear down necessary elements when app is complete
  private void teardown()
  {
    sc.stop();
  }

  /**
   * Method to read in data from an allowed input source/format and perform the query
   */
  public void performQuery() throws Exception
  {
    logger.info("Performing query: ");

    JavaRDD<MapWritable> inputRDD = null;
    if (dataInputFormat.equals(InputFormatConst.BASE_FORMAT))
    {
      inputRDD = readData();
    }
    else if (dataInputFormat.equals(InputFormatConst.ES))
    {
      inputRDD = readDataES();
    }
    performQuery(inputRDD);
  }

  /**
   * Method to read in the data from an allowed input format, filter, and return a RDD of MapWritable data elements
   */
  @SuppressWarnings("unchecked")
  public JavaRDD<MapWritable> readData() throws ClassNotFoundException, Exception
  {
    logger.info("Reading data ");

    JavaRDD<MapWritable> dataRDD;

    Job job = new Job();
    String baseQuery = SystemConfiguration.getProperty("pir.baseQuery");
    String jobName = "pirSpark_base_" + baseQuery + "_" + System.currentTimeMillis();
    job.setJobName(jobName);
    job.getConfiguration().setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    job.getConfiguration().set("query", baseQuery);

    logger.debug("queryType = " + bVars.getQueryInfo().getQueryType());
    logger.debug("QuerySchemaLoader.getSchemaNames().size() = " + QuerySchemaRegistry.getNames().size());
    for (String name : QuerySchemaRegistry.getNames())
    {
      logger.debug("schemaName = " + name);
    }

    QuerySchema qSchema = QuerySchemaRegistry.get(bVars.getQueryInfo().getQueryType());
    job.getConfiguration().set("dataSchemaName", qSchema.getDataSchemaName());
    job.getConfiguration().set("data.schemas", SystemConfiguration.getProperty("data.schemas"));

    // Set the inputFormatClass based upon the baseInputFormat property
    String classString = SystemConfiguration.getProperty("pir.baseInputFormat");
    Class<BaseInputFormat> inputClass = (Class<BaseInputFormat>) Class.forName(classString);
    if (!Class.forName("org.apache.pirk.inputformat.hadoop.BaseInputFormat").isAssignableFrom(inputClass))
    {
      throw new Exception("baseInputFormat class = " + classString + " does not extend BaseInputFormat");
    }
    job.setInputFormatClass(inputClass);

    FileInputFormat.setInputPaths(job, inputData);

    // Read data from hdfs
    JavaRDD<MapWritable> jsonRDD = sc.newAPIHadoopRDD(job.getConfiguration(), inputClass, Text.class, MapWritable.class).values().coalesce(numDataPartitions);

    // Filter out by the provided stopListFile entries
    dataRDD = jsonRDD.filter(new FilterData(accum, bVars));

    return dataRDD;
  }

  /**
   * Method to read in the data from elasticsearch, filter, and return a RDD of MapWritable data elements
   */
  @SuppressWarnings("unchecked")
  public JavaRDD<MapWritable> readDataES() throws Exception
  {
    logger.info("Reading data ");

    JavaRDD<MapWritable> dataRDD;

    Job job = new Job();
    String jobName = "pirSpark_ES_" + esQuery + "_" + System.currentTimeMillis();
    job.setJobName(jobName);
    job.getConfiguration().set("es.nodes", SystemConfiguration.getProperty("es.nodes"));
    job.getConfiguration().set("es.port", SystemConfiguration.getProperty("es.port"));
    job.getConfiguration().set("es.resource", esResource);
    job.getConfiguration().set("es.query", esQuery);

    JavaRDD<MapWritable> jsonRDD = sc.newAPIHadoopRDD(job.getConfiguration(), EsInputFormat.class, Text.class, MapWritable.class).values()
        .coalesce(numDataPartitions);

    // Filter out by the provided stopListFile entries
    dataRDD = jsonRDD.filter(new FilterData(accum, bVars));

    return dataRDD;
  }

  /**
   * Method to perform the query given an input RDD of MapWritables
   * 
   */
  public void performQuery(JavaRDD<MapWritable> inputRDD) throws PIRException
  {
    logger.info("Performing query: ");

    // If we are using distributed exp tables -- Create the expTable file in hdfs for this query, if it doesn't exist
    if ((queryInfo.getUseHDFSExpLookupTable() || useHDFSLookupTable) && query.getExpFileBasedLookup().isEmpty())
    {
      // <queryHash, <<power>,<element^power mod N^2>>
      JavaPairRDD<Integer,Iterable<Tuple2<Integer,BigInteger>>> expCalculations = ComputeExpLookupTable.computeExpTable(sc, fs, bVars, query, queryInput,
          outputDirExp);
    }

    // Extract the selectors for each dataElement based upon the query type
    // and perform a keyed hash of the selectors
    JavaPairRDD<Integer,ArrayList<BigInteger>> selectorHashToDocRDD = inputRDD.mapToPair(new HashSelectorsAndPartitionData(accum, bVars));

    // Group by hashed selector (row) -- can combine with the line above, separating for testing and benchmarking...
    JavaPairRDD<Integer,Iterable<ArrayList<BigInteger>>> selectorGroupRDD = selectorHashToDocRDD.groupByKey();

    // Calculate the encrypted row values for each row, emit <colNum, colVal> for each row
    JavaPairRDD<Long,BigInteger> encRowRDD;
    if (useModExpJoin)
    {
      // If we are pre-computing the modular exponentiation table and then joining the data partitions
      // for computing the encrypted rows

      // <queryHash, <<power>,<element^power mod N^2>>
      JavaPairRDD<Integer,Iterable<Tuple2<Integer,BigInteger>>> expCalculations = ComputeExpLookupTable.computeExpTable(sc, fs, bVars, query, queryInput,
          outputDirExp, useModExpJoin);

      JavaPairRDD<Integer,Tuple2<Iterable<Tuple2<Integer,BigInteger>>,Iterable<ArrayList<BigInteger>>>> encMapDataJoin = expCalculations.join(selectorGroupRDD);

      // Calculate the encrypted row values for each row, emit <colNum, colVal> for each row
      encRowRDD = encMapDataJoin.flatMapToPair(new EncRowCalcPrecomputedCache(accum, bVars));
    }
    else
    {
      encRowRDD = selectorGroupRDD.flatMapToPair(new EncRowCalc(accum, bVars));
    }

    // Multiply the column values by colNum: emit <colNum, finalColVal> and write the final result object
    encryptedColumnCalc(encRowRDD);

    // Teardown the context
    logger.info("Tearing down...");
    teardown();
    logger.info("Tear down complete");
  }

  // Method to compute the final encrypted columns
  private void encryptedColumnCalc(JavaPairRDD<Long,BigInteger> encRowRDD) throws PIRException
  {
    // Multiply the column values by colNum: emit <colNum, finalColVal>
    JavaPairRDD<Long,BigInteger> encColRDD;
    if (colMultReduceByKey)
    {
      encColRDD = encRowRDD.reduceByKey(new EncColMultReducer(accum, bVars), numColMultPartitions);
    }
    else
    {
      encColRDD = encRowRDD.groupByKey(numColMultPartitions).mapToPair(new EncColMultGroupedMapper(accum, bVars));
    }

    // Form the final response object
    Response response = new Response(queryInfo);
    Map<Long,BigInteger> encColResults = encColRDD.collectAsMap();
    logger.debug("encColResults.size() = " + encColResults.size());

    for (long colVal : encColResults.keySet())
    {
      response.addElement((int) colVal, encColResults.get(colVal));
      logger.debug("colNum = " + colVal + " column = " + encColResults.get(colVal).toString());
    }

    try
    {
      storage.store(outputFile, response);
    } catch (IOException e)
    {
      throw new RuntimeException(e);
    }
    accum.printAll();
  }
}
