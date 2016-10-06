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
package org.apache.pirk.responder.wideskies.spark.streaming;

import java.io.IOException;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pirk.inputformat.hadoop.BaseInputFormat;
import org.apache.pirk.inputformat.hadoop.InputFormatConst;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.responder.wideskies.spark.Accumulators;
import org.apache.pirk.responder.wideskies.spark.BroadcastVars;
import org.apache.pirk.responder.wideskies.spark.EncColMultGroupedMapper;
import org.apache.pirk.responder.wideskies.spark.EncColMultReducer;
import org.apache.pirk.responder.wideskies.spark.EncRowCalc;
import org.apache.pirk.responder.wideskies.spark.FilterData;
import org.apache.pirk.responder.wideskies.spark.HashSelectorsAndPartitionData;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.data.partitioner.DataPartitioner;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master class for the PIR query spark streaming application
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
public class ComputeStreamingResponse
{
  private static final Logger logger = LoggerFactory.getLogger(ComputeStreamingResponse.class);

  private String dataInputFormat = null;
  private String inputData = null;
  private String outputFile = null;
  private String outputDirExp = null;

  private String queryInput = null;
  private QuerySchema qSchema = null;

  private String esQuery = "none";
  private String esResource = "none";

  private FileSystem fs = null;
  private HadoopFileSystemStore storage = null;
  private JavaStreamingContext jssc = null;

  private boolean useQueueStream = false;

  private long windowLength = 0;

  private Accumulators accum = null;
  private BroadcastVars bVars = null;

  private int numDataPartitions = 0;
  private int numColMultPartitions = 0;

  private boolean colMultReduceByKey = false;

  public ComputeStreamingResponse(FileSystem fileSys) throws PIRException
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

    logger.info("outputFile = " + outputFile + " queryInputDir = " + queryInput + " stopListFile = " + stopListFile + " esQuery = " + esQuery + " esResource = "
        + esResource);

    // Pull the batchSeconds and windowLength parameters
    long batchSeconds = SystemConfiguration.getLongProperty("pir.sparkstreaming.batchSeconds", 30);
    windowLength = SystemConfiguration.getLongProperty("pir.sparkstreaming.windowLength", 60);
    if (windowLength % batchSeconds != 0)
    {
      throw new IllegalArgumentException("batchSeconds = " + batchSeconds + " must divide windowLength = " + windowLength);
    }
    useQueueStream = SystemConfiguration.getBooleanProperty("pir.sparkstreaming.useQueueStream", false);
    logger.info("useQueueStream = " + useQueueStream);

    // Set the necessary configurations
    SparkConf conf = new SparkConf().setAppName("SparkPIR").setMaster("yarn-cluster");
    conf.set("es.nodes", SystemConfiguration.getProperty("es.nodes", "none"));
    conf.set("es.port", SystemConfiguration.getProperty("es.port", "none"));
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.set("spark.streaming.stopGracefullyOnShutdown", SystemConfiguration.getProperty("spark.streaming.stopGracefullyOnShutdown", "false"));

    JavaSparkContext sc = new JavaSparkContext(conf);
    jssc = new JavaStreamingContext(sc, Durations.seconds(batchSeconds));

    // Setup, run query, teardown
    logger.info("Setting up for query run");
    try
    {
      setup();
    } catch (IOException e)
    {
      throw new PIRException("An error occurred setting up the streaming responder.", e);
    }
    logger.info("Setup complete");
  }

  // Setup for the accumulators and broadcast variables
  private void setup() throws IOException, PIRException
  {
    // Load the schemas
    DataSchemaLoader.initialize(true, fs);
    QuerySchemaLoader.initialize(true, fs);

    // Create the accumulators and broadcast variables
    accum = new Accumulators(jssc.sparkContext());
    bVars = new BroadcastVars(jssc.sparkContext());

    // Set the Query and QueryInfo broadcast variables
    Query query = storage.recall(queryInput, Query.class);
    QueryInfo queryInfo = query.getQueryInfo();
    bVars.setQuery(query);
    bVars.setQueryInfo(queryInfo);

    if (SystemConfiguration.getBooleanProperty("pir.allowAdHocQuerySchemas", false))
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
    bVars.setUseLocalCache(SystemConfiguration.getBooleanProperty("pir.useLocalCache", true));

    // Set the hit limit variables
    bVars.setLimitHitsPerSelector(SystemConfiguration.getBooleanProperty("pir.limitHitsPerSelector", false));
    bVars.setMaxHitsPerSelector(SystemConfiguration.getIntProperty("pir.maxHitsPerSelector", 100));

    // Set the number of data and column multiplication partitions
    numDataPartitions = SystemConfiguration.getIntProperty("pir.numDataPartitions", 1000);
    numColMultPartitions = SystemConfiguration.getIntProperty("pir.numColMultPartitions", numDataPartitions);

    // Whether or not we are performing a reduceByKey or a groupByKey->reduce for column multiplication
    colMultReduceByKey = SystemConfiguration.getBooleanProperty("pir.colMultReduceByKey", false);

    // Set the expDir
    bVars.setExpDir(outputDirExp);

    // Set the maxBatches
    int maxBatches = SystemConfiguration.getIntProperty("pir.sparkstreaming.maxBatches", -1);
    logger.info("maxBatches = " + maxBatches);
    bVars.setMaxBatches(maxBatches);
  }

  /**
   * Method to start the computation
   */
  public void start()
  {
    logger.info("Starting computation...");

    jssc.start();
    try
    {
      jssc.awaitTermination();
    } catch (InterruptedException e)
    {
      // Interrupted while waiting for termination
      Thread.interrupted();
    }
  }

  /**
   * Method to tear down necessary elements when app is complete
   */
  public void teardown()
  {
    logger.info("Tearing down...");
    jssc.stop();
    logger.info("Tear down complete");
  }

  /**
   * Method to read in data from an allowed input source/format and perform the query
   */
  public void performQuery() throws IOException, PIRException
  {
    logger.info("Performing query: ");

    JavaDStream<MapWritable> inputRDD = null;
    if (dataInputFormat.equals(InputFormatConst.BASE_FORMAT))
    {
      inputRDD = readData();
    }
    else if (dataInputFormat.equals(InputFormatConst.ES))
    {
      inputRDD = readDataES();
    }
    else
    {
      throw new PIRException("Unknown data input format " + dataInputFormat);
    }

    performQuery(inputRDD);
  }

  /**
   * Method to read in the data from an allowed input format, filter, and return a RDD of MapWritable data elements
   */
  @SuppressWarnings("unchecked")
  public JavaDStream<MapWritable> readData() throws IOException, PIRException
  {
    logger.info("Reading data ");

    Job job = Job.getInstance();
    String baseQuery = SystemConfiguration.getProperty("pir.baseQuery");
    String jobName = "pirSpark_base_" + baseQuery + "_" + System.currentTimeMillis();
    job.setJobName(jobName);
    job.getConfiguration().setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    job.getConfiguration().set("query", baseQuery);

    job.getConfiguration().set("dataSchemaName", qSchema.getDataSchemaName());
    job.getConfiguration().set("data.schemas", SystemConfiguration.getProperty("data.schemas"));

    // Set the inputFormatClass based upon the baseInputFormat property
    String classString = SystemConfiguration.getProperty("pir.baseInputFormat");
    Class<? extends BaseInputFormat<Text,MapWritable>> inputClass;
    try
    {
      inputClass = (Class<? extends BaseInputFormat<Text,MapWritable>>) Class.forName(classString);
    } catch (ClassNotFoundException | ClassCastException e)
    {
      throw new PIRException(classString + " cannot be instantiated or does not extend BaseInputFormat", e);
    }
    job.setInputFormatClass(inputClass);

    FileInputFormat.setInputPaths(job, inputData);

    // Read data from hdfs
    logger.info("useQueueStream = " + useQueueStream);
    JavaDStream<MapWritable> mwStream;
    if (useQueueStream)
    {
      Queue<JavaRDD<MapWritable>> rddQueue = new LinkedList<>();
      JavaRDD<MapWritable> rddIn = jssc.sparkContext().newAPIHadoopRDD(job.getConfiguration(), inputClass, Text.class, MapWritable.class).values()
          .coalesce(numDataPartitions);

      rddQueue.add(rddIn);
      mwStream = jssc.queueStream(rddQueue);
    }
    else
    {
      JavaPairInputDStream<Text,MapWritable> inputRDD = jssc.fileStream(inputData, Text.class, MapWritable.class, inputClass);
      mwStream = inputRDD.transform(new Function<JavaPairRDD<Text,MapWritable>,JavaRDD<MapWritable>>()
      {
        private static final long serialVersionUID = 1L;

        @Override
        public JavaRDD<MapWritable> call(JavaPairRDD<Text,MapWritable> pair) throws Exception
        {
          return pair.values();
        }
      }).repartition(numDataPartitions);
    }

    // Filter out by the provided stopListFile entries
    if (qSchema.getFilter() != null)
    {
      return mwStream.filter(new FilterData(accum, bVars));
    }

    return mwStream;
  }

  /**
   * Method to read in the data from elasticsearch, filter, and return a RDD of MapWritable data elements
   */
  @SuppressWarnings("unchecked")
  public JavaDStream<MapWritable> readDataES() throws IOException
  {
    logger.info("Reading data ");

    Job job = Job.getInstance();
    String jobName = "pirSpark_ES_" + esQuery + "_" + System.currentTimeMillis();
    job.setJobName(jobName);
    job.getConfiguration().set("es.nodes", SystemConfiguration.getProperty("es.nodes"));
    job.getConfiguration().set("es.port", SystemConfiguration.getProperty("es.port"));
    job.getConfiguration().set("es.resource", esResource);
    job.getConfiguration().set("es.query", esQuery);

    // Read data from hdfs
    JavaDStream<MapWritable> mwStream;
    if (useQueueStream)
    {
      Queue<JavaRDD<MapWritable>> rddQueue = new LinkedList<>();
      JavaRDD<MapWritable> rddIn = jssc.sparkContext().newAPIHadoopRDD(job.getConfiguration(), EsInputFormat.class, Text.class, MapWritable.class).values()
          .coalesce(numDataPartitions);
      rddQueue.add(rddIn);

      mwStream = jssc.queueStream(rddQueue);
    }
    else
    {
      JavaPairInputDStream<Text,MapWritable> inputRDD = jssc.fileStream(inputData, Text.class, MapWritable.class, EsInputFormat.class);
      mwStream = inputRDD.transform(new Function<JavaPairRDD<Text,MapWritable>,JavaRDD<MapWritable>>()
      {
        private static final long serialVersionUID = 1L;

        @Override
        public JavaRDD<MapWritable> call(JavaPairRDD<Text,MapWritable> pair) throws Exception
        {
          return pair.values();
        }
      }).repartition(numDataPartitions);
    }

    // Filter out by the provided stopListFile entries
    if (qSchema.getFilter() != null)
    {
      return mwStream.filter(new FilterData(accum, bVars));
    }
    else
    {
      return mwStream;
    }
  }

  /**
   * Method to perform the query given an input JavaDStream of JSON
   * 
   */
  public void performQuery(JavaDStream<MapWritable> input)
  {
    logger.info("Performing query: ");

    // Process non-overlapping windows of data of duration windowLength seconds
    // If we are using queue streams, there is no need to window
    if (!useQueueStream)
    {
      input.window(Durations.seconds(windowLength), Durations.seconds(windowLength));
    }

    // Extract the selectors for each dataElement based upon the query type
    // and perform a keyed hash of the selectors
    JavaPairDStream<Integer,List<BigInteger>> selectorHashToDocRDD = input.mapToPair(new HashSelectorsAndPartitionData(bVars));

    // Group by hashed selector (row) -- can combine with the line above, separating for testing and benchmarking...
    JavaPairDStream<Integer,Iterable<List<BigInteger>>> selectorGroupRDD = selectorHashToDocRDD.groupByKey();

    // Calculate the encrypted row values for each row, emit <colNum, colVal> for each row
    JavaPairDStream<Long,BigInteger> encRowRDD = selectorGroupRDD.flatMapToPair(new EncRowCalc(accum, bVars));

    // Multiply the column values by colNum: emit <colNum, finalColVal> and write the final result object
    encryptedColumnCalc(encRowRDD);

    // Start the streaming computation
    start();
  }

  // Method to compute the final encrypted columns
  private void encryptedColumnCalc(JavaPairDStream<Long,BigInteger> encRowRDD)
  {
    // Multiply the column values by colNum: emit <colNum, finalColVal>
    JavaPairDStream<Long,BigInteger> encColRDD;
    if (colMultReduceByKey)
    {
      encColRDD = encRowRDD.reduceByKey(new EncColMultReducer(bVars), numColMultPartitions);
    }
    else
    {
      encColRDD = encRowRDD.groupByKey(numColMultPartitions).mapToPair(new EncColMultGroupedMapper(bVars));
    }

    // Update the output name, by batch number
    bVars.setOutput(outputFile + "_" + accum.numBatchesGetValue());

    // Form and write the response object
    encColRDD.repartition(1).foreachRDD((VoidFunction<JavaPairRDD<Long,BigInteger>>) rdd -> {
      rdd.foreachPartition(new FinalResponseFunction(accum, bVars));

      int maxBatchesVar = bVars.getMaxBatches();
      if (maxBatchesVar != -1 && accum.numBatchesGetValue() == maxBatchesVar)
      {
        logger.info("num batches = maxBatches = " + maxBatchesVar + "; shutting down");
        System.exit(0);
      }

    });
  }
}
