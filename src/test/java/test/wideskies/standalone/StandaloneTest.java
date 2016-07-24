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
package test.wideskies.standalone;

import java.util.ArrayList;

import org.apache.pirk.schema.query.filter.StopListFilter;
import org.apache.pirk.test.utils.BaseTests;
import org.apache.pirk.test.utils.Inputs;
import org.apache.pirk.utils.SystemConfiguration;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test suite for stand alone testing - non Spark applications
 * <p>
 * Tests low side module and basic encryption, decryption mechanisms
 * <p>
 * Using a fixed 8-bit data partition size (consistent with the currently codebase)
 * <p>
 * Runs with useExpLookupTable = false as generating the lookup table takes too long for normal in-memory builds
 * 
 */
public class StandaloneTest
{
  private static final Logger logger = LoggerFactory.getLogger(StandaloneTest.class);

  private static final String STOPLIST_FILE = "testStopListFile";

  private String stopListFileProp = null;

  public StandaloneTest() throws Exception
  {
    // Create the stoplist file
    stopListFileProp = SystemConfiguration.getProperty("pir.stopListFile");
    SystemConfiguration.setProperty("pir.stopListFile", STOPLIST_FILE);
    String newSLFile = Inputs.createPIRStopList(null, false);
    SystemConfiguration.setProperty("pir.stopListFile", newSLFile);
    logger.info("stopListFileProp = " + stopListFileProp + " new prop = " + SystemConfiguration.getProperty("pir.stopListFile"));

    // Create data and query schemas
    Inputs.createSchemaFiles(StopListFilter.class.getName());
  }

  @Test
  public void runTests() throws Exception
  {
    ArrayList<JSONObject> dataElements = Inputs.createJSONDataElements();
    ArrayList<JSONObject> dataElementsRcode3 = Inputs.getRcode3JSONDataElements();
    
    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");

    // Run tests and use the embedded selector
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSHostnameQuery(dataElements, 1, false);
    BaseTests.testSRCIPQuery(dataElements, 2);
    BaseTests.testDNSIPQuery(dataElements, 3); // numThreads % num elements to encrypt != 0
    BaseTests.testDNSNXDOMAINQuery(dataElementsRcode3, 4); // numThreads % num elements to encrypt = 0
    
    //Test embedded QuerySchema
    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");
    BaseTests.testDNSHostnameQuery(dataElements, 1, false);
    
    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
    BaseTests.testDNSHostnameQuery(dataElements, 1, false);
    
    SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
    SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
    BaseTests.testDNSHostnameQuery(dataElements, 1, false);
    SystemConfiguration.setProperty("pir.embedQuerySchema", "false");
    
    // Run tests without using the embedded selector
    SystemConfiguration.setProperty("pirTest.embedSelector", "false");
    BaseTests.testDNSHostnameQuery(dataElements, 1, false);
    BaseTests.testSRCIPQuery(dataElements, 2);
    BaseTests.testDNSIPQuery(dataElements, 3);
    BaseTests.testDNSNXDOMAINQuery(dataElementsRcode3, 4);

    // Run using a false positive
    SystemConfiguration.setProperty("pirTest.embedSelector", "true");
    BaseTests.testDNSHostnameQuery(dataElements, 1, true);

    // Reset the stoplist file property
    SystemConfiguration.setProperty("pir.stopListFile", stopListFileProp);
  }
}
