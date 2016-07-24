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
import org.apache.pirk.responder.wideskies.ResponderCLI;
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
  public static void testJSONInputMR(FileSystem fs, ArrayList<JSONObject> dataElements) throws Exception
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
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 1);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2);

    BaseTests.testSRCIPQueryNoFilter(dataElements, fs, false, true, 2);

    // Test hit limits per selector
    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "true");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 3);
    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
    SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1000");

    // Test the local cache for modular exponentiation
    SystemConfiguration.setProperty("pir.useLocalCache", "true");
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2);
    BaseTests.testSRCIPQuery(dataElements, fs, false, true, 2);
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
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2);

    // Create exp table in hdfs
    SystemConfiguration.setProperty("mapreduce.map.memory.mb", "10000");
    SystemConfiguration.setProperty("mapreduce.reduce.memory.mb", "10000");
    SystemConfiguration.setProperty("mapreduce.map.java.opts", "-Xmx9000m");
    SystemConfiguration.setProperty("mapreduce.reduce.java.opts", "-Xmx9000m");

    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "true");
    SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");
    SystemConfiguration.setProperty("pir.expCreationSplits", "50");
    SystemConfiguration.setProperty("pir.numExpLookupPartitions", "150");
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2);

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

  public static void testESInputMR(FileSystem fs, ArrayList<JSONObject> dataElements) throws Exception
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
    SystemConfiguration.setProperty("pir.esResource", SystemConfiguration.getProperty(DistributedTestDriver.ES_PIR_INPUT_RESOURCE_PROPERTY));

    // Run tests
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 1);
    BaseTests.testSRCIPQuery(dataElements, fs, false, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 1);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, false, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, false, true, 2);

    // Change query for NXDOMAIN
    SystemConfiguration.setProperty("pir.esQuery", "?q=rcode:3");

    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, false, true, 3);
    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, false, true, 3);

    logger.info("Completed testESInputMR");
  }

  public static void testJSONInputSpark(FileSystem fs, ArrayList<JSONObject> dataElements) throws Exception
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
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 1);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 2);
    BaseTests.testSRCIPQuery(dataElements, fs, true, true, 2);

    BaseTests.testSRCIPQueryNoFilter(dataElements, fs, true, true, 2);

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
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 3);

    // Test the join functionality for the modular exponentiation table
    SystemConfiguration.setProperty("pir.useModExpJoin", "true");
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 3);
    SystemConfiguration.setProperty("pir.useModExpJoin", "false");

    // Test file based exp lookup table for modular exponentiation
    SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "true");
    SystemConfiguration.setProperty("pir.expCreationSplits", "500");
    SystemConfiguration.setProperty("pir.numExpLookupPartitions", "150");
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 2);
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

  public static void testESInputSpark(FileSystem fs, ArrayList<JSONObject> dataElements) throws Exception
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
    SystemConfiguration.setProperty("pir.esResource", SystemConfiguration.getProperty(DistributedTestDriver.ES_PIR_INPUT_RESOURCE_PROPERTY));

    // Run tests
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 1);
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 1);
    BaseTests.testSRCIPQuery(dataElements, fs, true, true, 2);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, true, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, true, true, 2);

    // Change query for NXDOMAIN
    SystemConfiguration.setProperty("pir.esQuery", "?q=rcode:3");

    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, true, true, 3);
    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, true, true, 3);

    logger.info("Completed testESInputSpark");
  }

  // Base method to perform query
  @SuppressWarnings("unused")
  public static ArrayList<QueryResponseJSON> performQuery(String queryType, ArrayList<String> selectors, FileSystem fs, boolean isSpark, int numThreads)
      throws Exception
  {
    logger.info("performQuery: ");

    String queryInputDir = SystemConfiguration.getProperty(DistributedTestDriver.PIR_QUERY_INPUT_DIR);
    String outputFile = SystemConfiguration.getProperty(DistributedTestDriver.OUTPUT_DIRECTORY_PROPERTY);
    Path outputFilePath = new Path(outputFile);
    if (fs.exists(outputFilePath))
    {
      fs.delete(new Path(outputFile), true);
    }
    SystemConfiguration.setProperty("pir.queryInput", queryInputDir);
    SystemConfiguration.setProperty("pir.outputFile", outputFile);
    SystemConfiguration.setProperty("pir.numReduceTasks", "1");
    SystemConfiguration.setProperty("pir.stopListFile", SystemConfiguration.getProperty(DistributedTestDriver.PIR_STOPLIST_FILE));

    ArrayList<QueryResponseJSON> results;

    // Create the temp result file
    File fileFinalResults = File.createTempFile("finalResultsFile", ".txt");
    fileFinalResults.deleteOnExit();
    logger.info("fileFinalResults = " + fileFinalResults.getAbsolutePath());

    boolean embedSelector = false;
    if (SystemConfiguration.getProperty("pirTest.embedSelector", "false").equals("true"))
    {
      embedSelector = true;
    }

    boolean useExpLookupTable = false;
    if (SystemConfiguration.getProperty("pirTest.useExpLookupTable", "false").equals("true"))
    {
      useExpLookupTable = true;
    }

    boolean useHDFSExpLookupTable = false;
    if (SystemConfiguration.getProperty("pirTest.useHDFSExpLookupTable", "false").equals("true"))
    {
      useHDFSExpLookupTable = true;
    }

    // Set the necessary objects
    QueryInfo queryInfo = new QueryInfo(BaseTests.queryNum, selectors.size(), BaseTests.hashBitSize, BaseTests.hashKey, BaseTests.dataPartitionBitSize,
        queryType, queryType + "_" + BaseTests.queryNum, BaseTests.paillierBitSize, useExpLookupTable, embedSelector, useHDFSExpLookupTable);

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
      // Build args
      String inputFormat = SystemConfiguration.getProperty("pir.dataInputFormat");
      logger.info("inputFormat = " + inputFormat);
      ArrayList<String> args = new ArrayList<>();
      args.add("-" + ResponderCLI.PLATFORM + "=spark");
      args.add("-" + ResponderCLI.DATAINPUTFORMAT + "=" + inputFormat);
      args.add("-" + ResponderCLI.QUERYINPUT + "=" + SystemConfiguration.getProperty("pir.queryInput"));
      args.add("-" + ResponderCLI.OUTPUTFILE + "=" + SystemConfiguration.getProperty("pir.outputFile"));
      args.add("-" + ResponderCLI.STOPLISTFILE + "=" + SystemConfiguration.getProperty("pir.stopListFile"));
      args.add("-" + ResponderCLI.USELOCALCACHE + "=" + SystemConfiguration.getProperty("pir.useLocalCache", "true"));
      args.add("-" + ResponderCLI.LIMITHITSPERSELECTOR + "=" + SystemConfiguration.getProperty("pir.limitHitsPerSelector", "false"));
      args.add("-" + ResponderCLI.MAXHITSPERSELECTOR + "=" + SystemConfiguration.getProperty("pir.maxHitsPerSelector", "1000"));
      args.add("-" + ResponderCLI.QUERYSCHEMAS + "=" + Inputs.HDFS_QUERY_FILES);
      args.add("-" + ResponderCLI.DATASCHEMAS + "=" + Inputs.DATA_SCHEMA_FILE_HDFS);
      args.add("-" + ResponderCLI.NUMEXPLOOKUPPARTS + "=" + SystemConfiguration.getProperty("pir.numExpLookupPartitions", "100"));
      args.add("-" + ResponderCLI.USEMODEXPJOIN + "=" + SystemConfiguration.getProperty("pir.useModExpJoin", "false"));
      args.add("-" + ResponderCLI.NUMCOLMULTPARTITIONS + "=" + SystemConfiguration.getProperty("pir.numColMultPartitions", "20"));
      args.add("-" + ResponderCLI.COLMULTREDUCEBYKEY + "=" + SystemConfiguration.getProperty("pir.colMultReduceByKey", "false"));
      if (inputFormat.equals(InputFormatConst.BASE_FORMAT))
      {
        args.add("-" + ResponderCLI.INPUTDATA + "=" + SystemConfiguration.getProperty("pir.inputData"));
        args.add("-" + ResponderCLI.BASEQUERY + "=" + SystemConfiguration.getProperty("pir.baseQuery"));
        args.add("-" + ResponderCLI.BASEINPUTFORMAT + "=" + SystemConfiguration.getProperty("pir.baseInputFormat"));
      }
      else if (inputFormat.equals(InputFormatConst.ES))
      {
        args.add("-" + ResponderCLI.ESQUERY + "=" + SystemConfiguration.getProperty("pir.esQuery"));
        args.add("-" + ResponderCLI.ESRESOURCE + "=" + SystemConfiguration.getProperty("pir.esResource"));
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
    results = TestUtils.readResultsFile(fileFinalResults);

    // Reset data and query schema properties
    SystemConfiguration.setProperty("data.schemas", dataSchemaProp);
    SystemConfiguration.setProperty("query.schemas", querySchemaProp);

    // Clean up output dir in hdfs
    fs.delete(new Path(outputFile), true);

    return results;
  }
}
