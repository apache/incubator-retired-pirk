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
package org.apache.pirk.test.distributed.testsuite;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.inputformat.hadoop.InputFormatConst;
import org.apache.pirk.inputformat.hadoop.json.JSONInputFormatBase;
import org.apache.pirk.querier.wideskies.Querier;
import org.apache.pirk.querier.wideskies.decrypt.DecryptResponse;
import org.apache.pirk.querier.wideskies.encrypt.EncryptQuery;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.responder.wideskies.ResponderProps;
import org.apache.pirk.responder.wideskies.mapreduce.ComputeResponseTool;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.apache.pirk.test.distributed.DistributedTestDriver;
import org.apache.pirk.test.utils.BaseTests;
import org.apache.pirk.test.utils.Inputs;
import org.apache.pirk.test.utils.TestUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.apache.spark.launcher.SparkLauncher;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed test class for PIR
 * 
 */
public class DistTestSuite
{
  private static final Logger logger = LoggerFactory.getLogger(DistTestSuite.class);

  // This method also tests all non-query specific configuration options/properties
  // for the MapReduce version of PIR
  public static void testJSONInputMR(FileSystem fs, List<JSONObject> dataElements) throws Exception
  {
    logger.info("Starting testJSONInputMR");

    // Pull original data and query schema properties
    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");
    SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");

    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "100");

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");

    // Set up base configs
    SystemConfiguration.setProperty("pir.dataInputFormat", InputFormatConst.BASE_FORMAT);
    SystemConfiguration.setProperty("pir.inputData", SystemConfiguration.getProperty(DistributedTestDriver.JSON_PIR_INPUT_FILE_PROPERTY));
    SystemConfiguration.setProperty("pir.baseQuery", "?q=rcode:0");

    // Run tests
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 1);
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 1, false);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2, false);

    BaseTests.testSRCIPQueryNoFilter(dataElements, fs, false, true, 2, false);

    // Test hit limits per selector
    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "true");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 3);
    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1000");

    // Test the local cache for modular exponentiation
    SystemConfiguration.setProperty("pir.useLocalCache", "true");
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2, false);
    BaseTests.testSRCIPQuery(dataElements, fs, false, true, 2, false);
    SystemConfiguration.setProperty("pir.useLocalCache", "false");

    // Change query for NXDOMAIN
    SystemConfiguration.setProperty("pir.baseQuery", "?q=rcode:3");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, false, true, 2);
    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, false, true, 2);
    SystemConfiguration.setProperty("pir.baseQuery", "?q=rcode:0");

    // Test the expTable cases
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");

    // In memory table
    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");
    SystemConfiguration.setProperty("pirTest.useExpLookupTable", "true");
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2, false);

    // Create exp table in hdfs
    SystemConfiguration.setProperty("mapreduce.map.memory.mb", "10000");
    SystemConfiguration.setProperty("mapreduce.reduce.memory.mb", "10000");
    SystemConfiguration.setProperty("mapreduce.map.java.opts", "-Xmx9000m");
    SystemConfiguration.setProperty("mapreduce.reduce.java.opts", "-Xmx9000m");

    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "true");
    SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");
    SystemConfiguration.setProperty("pir.expCreationSplits", "50");
    SystemConfiguration.setProperty("pir.numExpLookupPartitions", "150");
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2, false);

    // Reset exp properties
    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");
    SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");

    // Reset property
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");

    // Test embedded QuerySchema
    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 1);

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 1);

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 1);

    logger.info("Completed testJSONInputMR");
  }

  public static void testESInputMR(FileSystem fs, List<JSONObject> dataElements) throws Exception
  {
    logger.info("Starting testESInputMR");

    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");
    SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");

    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1000");

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");

    // Set up ES configs
    SystemConfiguration.setProperty("pir.dataInputFormat", InputFormatConst.ES);
    SystemConfiguration.setProperty("pir.esQuery", "?q=rcode:0");
    SystemConfiguration.setProperty("pir.esResource", SystemConfiguration.getProperty(DistributedTestDriver.ES_INPUT_RESOURCE_PROPERTY));

    // Run tests
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 1);
    BaseTests.testSRCIPQuery(dataElements, fs, false, true, 2, false);
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 1, false);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2, false);

    // Change query for NXDOMAIN
    SystemConfiguration.setProperty("pir.esQuery", "?q=rcode:3");

    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, false, true, 3);
    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, false, true, 3);

    logger.info("Completed testESInputMR");
  }

  public static void testJSONInputSpark(FileSystem fs, List<JSONObject> dataElements) throws Exception
  {
    logger.info("Starting testJSONInputSpark");

    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");
    SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");
    SystemConfiguration.setProperty("pir.useModExpJoin", "false");

    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1000");

    SystemConfiguration.setProperty("pir.numColMultPartitions", "20");
    SystemConfiguration.setProperty("pir.colMultReduceByKey", "false");

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");

    // Set up JSON configs
    SystemConfiguration.setProperty("pir.dataInputFormat", InputFormatConst.BASE_FORMAT);
    SystemConfiguration.setProperty("pir.inputData", SystemConfiguration.getProperty(DistributedTestDriver.JSON_PIR_INPUT_FILE_PROPERTY));
    SystemConfiguration.setProperty("pir.baseQuery", "?q=rcode:0");

    // Run tests
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 1);
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 1, false);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 2, false);
    BaseTests.testSRCIPQuery(dataElements, fs, true, true, 2, false);

    BaseTests.testSRCIPQueryNoFilter(dataElements, fs, true, true, 2, false);

    // Test embedded QuerySchema
    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 1);

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 1);

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 1);
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");

    // Test pad columns
    SystemConfiguration.setProperty("pir.padEmptyColumns", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 1);
    SystemConfiguration.setProperty("pir.padEmptyColumns", "false");

    // Test hit limits per selector
    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "true");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 3);
    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1000");

    // Test the local cache for modular exponentiation
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    SystemConfiguration.setProperty("pir.useLocalCache", "true");
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 3, false);

    // Test the join functionality for the modular exponentiation table
    SystemConfiguration.setProperty("pir.useModExpJoin", "true");
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 3, false);
    SystemConfiguration.setProperty("pir.useModExpJoin", "false");

    // Test file based exp lookup table for modular exponentiation
    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "true");
    SystemConfiguration.setProperty("pir.expCreationSplits", "500");
    SystemConfiguration.setProperty("pir.numExpLookupPartitions", "150");
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 2, false);
    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");

    // Change query for NXDOMAIN
    SystemConfiguration.setProperty("pir.baseQuery", "?q=rcode:3");

    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, true, true, 3);
    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, true, true, 3);

    // Test with reduceByKey for column mult
    SystemConfiguration.setProperty("pir.colMultReduceByKey", "true");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, true, true, 3);

    logger.info("Completed testJSONInputSpark");
  }

  public static void testESInputSpark(FileSystem fs, List<JSONObject> dataElements) throws Exception
  {
    logger.info("Starting testESInputSpark");

    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");
    SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");

    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1000");

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");

    // Set up ES configs
    SystemConfiguration.setProperty("pir.dataInputFormat", InputFormatConst.ES);
    SystemConfiguration.setProperty("pir.esQuery", "?q=rcode:0");
    SystemConfiguration.setProperty("pir.esResource", SystemConfiguration.getProperty(DistributedTestDriver.ES_INPUT_RESOURCE_PROPERTY));

    // Run tests
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 1);
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 1, false);
    BaseTests.testSRCIPQuery(dataElements, fs, true, true, 2, false);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 2, false);

    // Change query for NXDOMAIN
    SystemConfiguration.setProperty("pir.esQuery", "?q=rcode:3");

    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, true, true, 3);
    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, true, true, 3);

    logger.info("Completed testESInputSpark");
  }

  public static void testSparkStreaming(FileSystem fs, List<JSONObject> pirDataElements) throws Exception
  {
    testJSONInputSparkStreaming(fs, pirDataElements);
    testESInputSparkStreaming(fs, pirDataElements);
  }

  public static void testJSONInputSparkStreaming(FileSystem fs, List<JSONObject> pirDataElements) throws Exception
  {
    logger.info("Starting testJSONInputSparkStreaming");

    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1000");

    SystemConfiguration.setProperty("pir.numColMultPartitions", "20");
    SystemConfiguration.setProperty("pir.colMultReduceByKey", "false");

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");

    SystemConfiguration.setProperty("pirTest.embedSelector", "true");

    SystemConfiguration.setProperty("pir.sparkstreaming.batchSeconds", "30");
    SystemConfiguration.setProperty("pir.sparkstreaming.windowLength", "60");
    SystemConfiguration.setProperty("pir.sparkstreaming.useQueueStream", "true");
    SystemConfiguration.setProperty("pir.sparkstreaming.maxBatches", "1");

    SystemConfiguration.setProperty("spark.streaming.stopGracefullyOnShutdown", "false");

    // Set up JSON configs
    SystemConfiguration.setProperty("pir.dataInputFormat", InputFormatConst.BASE_FORMAT);
    SystemConfiguration.setProperty("pir.inputData", SystemConfiguration.getProperty(DistributedTestDriver.JSON_PIR_INPUT_FILE_PROPERTY));
    SystemConfiguration.setProperty("pir.baseQuery", "?q=rcode:0");

    // Run tests
    BaseTests.testDNSHostnameQuery(pirDataElements, fs, true, true, 1, false, true);
    BaseTests.testDNSIPQuery(pirDataElements, fs, true, true, 1, true);
    BaseTests.testSRCIPQuery(pirDataElements, fs, true, true, 3, true);
    BaseTests.testSRCIPQueryNoFilter(pirDataElements, fs, true, true, 2, true);

    logger.info("Completed testJSONInputSparkStreaming");
  }

  public static void testESInputSparkStreaming(FileSystem fs, List<JSONObject> pirDataElements) throws Exception
  {
    logger.info("Starting testESInputSparkStreaming");

    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");
    SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");

    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1000");

    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");

    SystemConfiguration.setProperty("pir.sparkstreaming.batchSeconds", "30");
    SystemConfiguration.setProperty("pir.sparkstreaming.windowLength", "60");
    SystemConfiguration.setProperty("pir.sparkstreaming.useQueueStream", "true");
    SystemConfiguration.setProperty("pir.sparkstreaming.maxBatches", "1");

    SystemConfiguration.setProperty("spark.streaming.stopGracefullyOnShutdown", "false");

    // Set up ES configs
    SystemConfiguration.setProperty("pir.dataInputFormat", InputFormatConst.ES);
    SystemConfiguration.setProperty("pir.esQuery", "?q=rcode:0");
    SystemConfiguration.setProperty("pir.esResource", SystemConfiguration.getProperty(DistributedTestDriver.ES_INPUT_RESOURCE_PROPERTY));

    // Run tests
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSHostnameQuery(pirDataElements, fs, true, true, 1, false, true);
    BaseTests.testDNSIPQuery(pirDataElements, fs, true, true, 1, true);
    BaseTests.testSRCIPQuery(pirDataElements, fs, true, true, 3, true);

    logger.info("Completed testESInputSparkStreaming");
  }

  // Base method to perform query
  // TODO: This could be changed to pass in the platform instead of isSpark and isStreaming...
  @SuppressWarnings("unused")
  public static List<QueryResponseJSON> performQuery(String queryType, ArrayList<String> selectors, FileSystem fs, boolean isSpark, int numThreads,
      boolean isStreaming) throws Exception
  {
    logger.info("performQuery: ");

    String queryInputDir = SystemConfiguration.getProperty(DistributedTestDriver.PIR_QUERY_INPUT_DIR);
    String outputFile = SystemConfiguration.getProperty(DistributedTestDriver.OUTPUT_DIRECTORY_PROPERTY);
    fs.delete(new Path(outputFile), true); // Ensure old output does not exist.

    SystemConfiguration.setProperty("pir.queryInput", queryInputDir);
    SystemConfiguration.setProperty("pir.outputFile", outputFile);
    SystemConfiguration.setProperty("pir.numReduceTasks", "1");
    SystemConfiguration.setProperty("pir.stopListFile", SystemConfiguration.getProperty(DistributedTestDriver.PIR_STOPLIST_FILE));

    // Create the temp result file
    File fileFinalResults = File.createTempFile("finalResultsFile", ".txt");
    fileFinalResults.deleteOnExit();
    logger.info("fileFinalResults = " + fileFinalResults.getAbsolutePath());

    boolean embedSelector = SystemConfiguration.getBooleanProperty("pirTest.embedSelector", false);
    boolean useExpLookupTable = SystemConfiguration.getBooleanProperty("pirTest.useExpLookupTable", false);
    boolean useHDFSExpLookupTable = SystemConfiguration.getBooleanProperty("pirTest.useHDFSExpLookupTable", false);

    // Set the necessary objects
    QueryInfo queryInfo = new QueryInfo(BaseTests.queryIdentifier, selectors.size(), BaseTests.hashBitSize, BaseTests.hashKey, BaseTests.dataPartitionBitSize,
        queryType, useExpLookupTable, embedSelector, useHDFSExpLookupTable);

    Paillier paillier = new Paillier(BaseTests.paillierBitSize, BaseTests.certainty);

    // Perform the encryption
    logger.info("Performing encryption of the selectors - forming encrypted query vectors:");
    EncryptQuery encryptQuery = new EncryptQuery(queryInfo, selectors, paillier);
    encryptQuery.encrypt(numThreads);
    logger.info("Completed encryption of the selectors - completed formation of the encrypted query vectors:");

    // Grab the necessary objects
    Querier querier = encryptQuery.getQuerier();
    Query query = encryptQuery.getQuery();

    // Write the Querier object to a file
    Path queryInputDirPath = new Path(queryInputDir);
    new HadoopFileSystemStore(fs).store(queryInputDirPath, query);
    fs.deleteOnExit(queryInputDirPath);

    // Grab the original data and query schema properties to reset upon completion
    String dataSchemaProp = SystemConfiguration.getProperty("data.schemas");
    String querySchemaProp = SystemConfiguration.getProperty("query.schemas");

    // Get the correct input format class name
    JSONInputFormatBase jFormat = new JSONInputFormatBase();
    String jsonBaseInputFormatString = jFormat.getClass().getName();
    SystemConfiguration.setProperty("pir.baseInputFormat", jsonBaseInputFormatString);

    // Submitting the tool for encrypted query
    logger.info("Performing encrypted query:");
    if (isSpark)
    {
      logger.info("spark.home = " + SystemConfiguration.getProperty("spark.home"));

      // Build args
      String inputFormat = SystemConfiguration.getProperty("pir.dataInputFormat");
      logger.info("inputFormat = " + inputFormat);
      ArrayList<String> args = new ArrayList<>();
      if (isStreaming)
      {
        logger.info("platform = sparkstreaming");
        args.add("-" + ResponderProps.PLATFORM + "=sparkstreaming");
        args.add("-" + ResponderProps.BATCHSECONDS + "=" + SystemConfiguration.getProperty("pir.sparkstreaming.batchSeconds", "30"));
        args.add("-" + ResponderProps.WINDOWLENGTH + "=" + SystemConfiguration.getProperty("pir.sparkstreaming.windowLength", "60"));
        args.add("-" + ResponderProps.MAXBATCHES + "=" + SystemConfiguration.getProperty("pir.sparkstreaming.maxBatches", "-1"));
        args.add("-" + ResponderProps.STOPGRACEFULLY + "=" + SystemConfiguration.getProperty("spark.streaming.stopGracefullyOnShutdown", "false"));
      }
      else
      {
        logger.info("platform = spark");
        args.add("-" + ResponderProps.PLATFORM + "=spark");
      }
      args.add("-" + ResponderProps.DATAINPUTFORMAT + "=" + inputFormat);
      args.add("-" + ResponderProps.QUERYINPUT + "=" + SystemConfiguration.getProperty("pir.queryInput"));
      args.add("-" + ResponderProps.OUTPUTFILE + "=" + SystemConfiguration.getProperty("pir.outputFile"));
      args.add("-" + ResponderProps.STOPLISTFILE + "=" + SystemConfiguration.getProperty("pir.stopListFile"));
      args.add("-" + ResponderProps.USELOCALCACHE + "=" + SystemConfiguration.getProperty("pir.useLocalCache", "true"));
      args.add("-" + ResponderProps.LIMITHITSPERSELECTOR + "=" + SystemConfiguration.getProperty("pir.limitHitsPerSelector", "false"));
      args.add("-" + ResponderProps.MAXHITSPERSELECTOR + "=" + SystemConfiguration.getProperty("pir.maxHitsPerSelector", "1000"));
      args.add("-" + ResponderProps.QUERYSCHEMAS + "=" + Inputs.HDFS_QUERY_FILES);
      args.add("-" + ResponderProps.DATASCHEMAS + "=" + Inputs.DATA_SCHEMA_FILE_HDFS);
      args.add("-" + ResponderProps.NUMEXPLOOKUPPARTS + "=" + SystemConfiguration.getProperty("pir.numExpLookupPartitions", "100"));
      args.add("-" + ResponderProps.USEMODEXPJOIN + "=" + SystemConfiguration.getProperty("pir.useModExpJoin", "false"));
      args.add("-" + ResponderProps.NUMCOLMULTPARTITIONS + "=" + SystemConfiguration.getProperty("pir.numColMultPartitions", "20"));
      args.add("-" + ResponderProps.COLMULTREDUCEBYKEY + "=" + SystemConfiguration.getProperty("pir.colMultReduceByKey", "false"));
      if (inputFormat.equals(InputFormatConst.BASE_FORMAT))
      {
        args.add("-" + ResponderProps.INPUTDATA + "=" + SystemConfiguration.getProperty("pir.inputData"));
        args.add("-" + ResponderProps.BASEQUERY + "=" + SystemConfiguration.getProperty("pir.baseQuery"));
        args.add("-" + ResponderProps.BASEINPUTFORMAT + "=" + SystemConfiguration.getProperty("pir.baseInputFormat"));
      }
      else if (inputFormat.equals(InputFormatConst.ES))
      {
        args.add("-" + ResponderProps.ESQUERY + "=" + SystemConfiguration.getProperty("pir.esQuery"));
        args.add("-" + ResponderProps.ESRESOURCE + "=" + SystemConfiguration.getProperty("pir.esResource"));
      }

      for (String arg : args)
      {
        logger.info("arg = " + arg);
      }

      // Run spark application
      Process sLauncher = new SparkLauncher().setAppResource(SystemConfiguration.getProperty("jarFile"))
          .setSparkHome(SystemConfiguration.getProperty("spark.home")).setMainClass("org.apache.pirk.responder.wideskies.ResponderDriver")
          .addAppArgs(args.toArray(new String[args.size()])).setMaster("yarn-cluster").setConf(SparkLauncher.EXECUTOR_MEMORY, "2g")
          .setConf(SparkLauncher.DRIVER_MEMORY, "2g").setConf(SparkLauncher.EXECUTOR_CORES, "1").launch();
      sLauncher.waitFor();
    }
    else
    {
      SystemConfiguration.setProperty("data.schemas", Inputs.DATA_SCHEMA_FILE_HDFS);
      SystemConfiguration.setProperty("query.schemas", Inputs.HDFS_QUERY_FILES);

      ComputeResponseTool responseTool = new ComputeResponseTool();
      ToolRunner.run(responseTool, new String[] {});
    }
    logger.info("Completed encrypted query");

    // Perform decryption
    // Reconstruct the necessary objects from the files
    logger.info("Performing decryption; writing final results file");
    Response response = new HadoopFileSystemStore(fs).recall(outputFile, Response.class);

    // Perform decryption and output the result file
    DecryptResponse decryptResponse = new DecryptResponse(response, querier);
    decryptResponse.decrypt(numThreads);
    decryptResponse.writeResultFile(fileFinalResults);
    logger.info("Completed performing decryption and writing final results file");

    // Read in results
    logger.info("Reading in and checking results");
    List<QueryResponseJSON> results = TestUtils.readResultsFile(fileFinalResults);

    // Reset data and query schema properties
    SystemConfiguration.setProperty("data.schemas", dataSchemaProp);
    SystemConfiguration.setProperty("query.schemas", querySchemaProp);

    // Clean up output dir in hdfs
    fs.delete(new Path(outputFile), true);

    return results;
  }
}
