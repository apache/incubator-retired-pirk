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

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.pirk.inputformat.hadoop.InputFormatConst;
import org.apache.pirk.test.distributed.DistributedTestDriver;
import org.apache.pirk.test.utils.BaseTests;
import org.apache.pirk.utils.SystemConfiguration;
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
  private static final String MAPREDUCE = "mapreduce";
  private static final String SPARK = "spark";
  private static final String SPARKSTREAMING = "sparkstreaming";

  public static void testJSONInput(String platform, FileSystem fs, List<JSONObject> dataElements) throws Exception
  {
    logger.info("Starting testJSONInput for " + platform);

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
    BaseTests.testDNSHostnameQuery(dataElements, fs, platform, true, 1);
    BaseTests.testDNSIPQuery(dataElements, fs, platform, true, 1);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, platform, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, platform, true, 2);
    BaseTests.testSRCIPQueryNoFilter(dataElements, fs, platform, true, 2);

    if (!platform.equals(SPARKSTREAMING)) {
      // Test hit limits per selector
      // Test hit limits per selector
      SystemConfiguration.setProperty("pir.limitHitsPerSelector", "true");
      SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1");
      BaseTests.testDNSHostnameQuery(dataElements, fs, platform, true, 3);
      SystemConfiguration.setProperty("pir.limitHitsPerSelector", "false");
      SystemConfiguration.setProperty("pir.maxHitsPerSelector", "1000");

      // Test the local cache for modular exponentiation
      SystemConfiguration.setProperty("pir.useLocalCache", "true");
      BaseTests.testDNSIPQuery(dataElements, fs, platform, true, 2);
      BaseTests.testSRCIPQuery(dataElements, fs, platform, true, 2);
      SystemConfiguration.setProperty("pir.useLocalCache", "false");

      // Change query for NXDOMAIN
      SystemConfiguration.setProperty("pir.baseQuery", "?q=rcode:3");
      BaseTests.testDNSNXDOMAINQuery(dataElements, fs, platform, true, 2);
      SystemConfiguration.setProperty("pirTest.embedSelector", "false");
      BaseTests.testDNSNXDOMAINQuery(dataElements, fs, platform, true, 2);
      SystemConfiguration.setProperty("pir.baseQuery", "?q=rcode:0");

      // Test the expTable cases
      SystemConfiguration.setProperty("pirTest.embedSelector", "true");

      // In memory table
      if (platform.equals(MAPREDUCE)) {
        SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");
        SystemConfiguration.setProperty("pirTest.useExpLookupTable", "true");
        BaseTests.testDNSIPQuery(dataElements, fs, MAPREDUCE, true, 2);
      }
      // Create exp table in hdfs
      SystemConfiguration.setProperty("mapreduce.map.memory.mb", "10000");
      SystemConfiguration.setProperty("mapreduce.reduce.memory.mb", "10000");
      SystemConfiguration.setProperty("mapreduce.map.java.opts", "-Xmx9000m");
      SystemConfiguration.setProperty("mapreduce.reduce.java.opts", "-Xmx9000m");

      SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "true");
      SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");
      SystemConfiguration.setProperty("pir.expCreationSplits", "50");
      SystemConfiguration.setProperty("pir.numExpLookupPartitions", "150");
      BaseTests.testDNSIPQuery(dataElements, fs, platform, true, 2);

      // Reset exp properties
      SystemConfiguration.setProperty("pirTest.useHDFSExpLookupTable", "false");
      SystemConfiguration.setProperty("pirTest.useExpLookupTable", "false");

      // Reset property
      SystemConfiguration.setProperty("pirTest.embedSelector", "true");

      // Test embedded QuerySchema
      SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
      SystemConfiguration.setProperty("pir.embedQuerySchema", "false");
      BaseTests.testDNSHostnameQuery(dataElements, fs, platform, true, 1);

      SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
      SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
      BaseTests.testDNSHostnameQuery(dataElements, fs, platform, true, 1);

      SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
      SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
      BaseTests.testDNSHostnameQuery(dataElements, fs, platform, true, 1);
    }
    
    logger.info("Completed testJSONInput for platform {}", platform);
  }

  public static void testESInput(String platform, FileSystem fs, List<JSONObject> dataElements) throws Exception
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
    BaseTests.testDNSHostnameQuery(dataElements, fs, platform, true, 1);
    BaseTests.testSRCIPQuery(dataElements, fs, platform, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, platform, true, 1);

    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, fs, platform, true, 2);
    BaseTests.testDNSIPQuery(dataElements, fs, platform, true, 2);

    // Change query for NXDOMAIN
    SystemConfiguration.setProperty("pir.esQuery", "?q=rcode:3");

    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, platform, true, 3);
    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSNXDOMAINQuery(dataElements, fs, platform, true, 3);

    logger.info("Completed testESInputMR");
  }

}
