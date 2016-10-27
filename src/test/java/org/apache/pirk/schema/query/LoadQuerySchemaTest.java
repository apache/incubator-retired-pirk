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
package org.apache.pirk.schema.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.data.partitioner.IPDataPartitioner;
import org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner;
import org.apache.pirk.schema.query.filter.StopListFilter;
import org.apache.pirk.test.utils.Inputs;
import org.apache.pirk.test.utils.TestUtils;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Test suite for LoadQuerySchema and QuerySchema
 */
public class LoadQuerySchemaTest
{
  private static final Logger logger = LoggerFactory.getLogger(LoadQuerySchemaTest.class);

  private final String querySchemaFile = "querySchemaFile";
  private final String dataSchemaName = "fakeDataSchema";
  private final String querySchemaName = "fakeQuerySchema";

  private final String element1 = "elementName1";
  private final String element2 = "elementName2";
  private final String element3 = "elementName3";
  private final String element4 = "elementName4";

  private final List<String> queryElements = Arrays.asList(element1, element2, element3);
  private final List<String> filterElements = Collections.singletonList(element2);

  @Test
  public void testGeneralSchemaLoad() throws Exception
  {
    logger.info("Starting testGeneralSchemaLoad: ");

    // Pull off the properties and reset upon completion
    String dataSchemasProp = SystemConfiguration.getProperty("data.schemas", "none");
    String querySchemasProp = SystemConfiguration.getProperty("query.schemas", "none");
    String stopListFileProp = SystemConfiguration.getProperty("pir.stopListFile");

    // Create the stoplist file
    createStopListFile();

    // Create the data schema used and force it to load
    createDataSchema("dataSchemaFile");
    DataSchemaLoader.initialize();

    // Create the query schema used and force it to load
    TestUtils.createQuerySchema(querySchemaFile, querySchemaName, dataSchemaName, element4, queryElements, filterElements, StopListFilter.class.getName());
    QuerySchemaLoader.initialize();

    // Check the entries
    QuerySchema qSchema = QuerySchemaRegistry.get(querySchemaName);

    assertEquals(querySchemaName, qSchema.getSchemaName());
    assertEquals(dataSchemaName, qSchema.getDataSchemaName());
    assertEquals(element4, qSchema.getSelectorName());

    assertEquals(StopListFilter.class.getName(), qSchema.getFilterTypeName());
    assertTrue("Filter class instance must be StopListFilter", qSchema.getFilter() instanceof StopListFilter);

    assertEquals(3, qSchema.getElementNames().size());
    for (String item : qSchema.getElementNames())
    {
      assertTrue("elementNames: item = " + item + " must equal one of: " + element1 + ", " + element2 + ", or " + element3,
          item.equals(element1) || item.equals(element2) || item.equals(element3));
    }
    assertEquals(1, qSchema.getFilteredElementNames().size());
    for (String item : qSchema.getFilteredElementNames())
    {
      assertEquals("filterElementNames: item = " + item + " must equal " + element2, item, element2);
    }

    // one string, array IPs, array integers
    int stringSize = Integer.parseInt(SystemConfiguration.getProperty("pir.stringBits"));
    int arrayMult = Integer.parseInt(SystemConfiguration.getProperty("pir.numReturnArrayElements"));
    int dataElementSize = stringSize + 32 * arrayMult + 32 * arrayMult;
    assertEquals(dataElementSize, qSchema.getDataElementSize());

    // Reset original query and data schema properties
    SystemConfiguration.setProperty("data.schemas", dataSchemasProp);
    SystemConfiguration.setProperty("query.schemas", querySchemasProp);
    SystemConfiguration.setProperty("pir.stopListFile", stopListFileProp);

    // Force the query and data schemas to load their original values
    if (!dataSchemasProp.equals("none"))
    {
      DataSchemaLoader.initialize();
    }

    if (!querySchemasProp.equals("none"))
    {
      QuerySchemaLoader.initialize();
    }

    logger.info("Finished testGeneralSchemaLoad: ");
  }

  @Test
  public void testGeneralSchemaLoadWithAdditionalFields() throws Exception
  {
    logger.info("Starting testGeneralSchemaLoadWithAdditionalFields: ");

    // Pull off the properties and reset upon completion
    String dataSchemasProp = SystemConfiguration.getProperty("data.schemas", "none");
    String querySchemasProp = SystemConfiguration.getProperty("query.schemas", "none");

    // Create the data schema used and force it to load
    createDataSchema("dataSchemaFile");
    DataSchemaLoader.initialize();

    // Create the additionalFields
    HashMap<String,String> additionalFields = new HashMap<>();
    additionalFields.put("key1", "value1");
    additionalFields.put("key2", "value2");

    // Create the query schema used and force it to load
    TestUtils.createQuerySchema(querySchemaFile, querySchemaName, dataSchemaName, element4, queryElements, filterElements, null, true, null, false,
        additionalFields);
    QuerySchemaLoader.initialize();

    // Check the entries
    QuerySchema qSchema = QuerySchemaRegistry.get(querySchemaName);
    assertNotNull("qSchema is null", qSchema);

    Map<String,String> schemaAdditionalFields = qSchema.getAdditionalFields();
    assertEquals(schemaAdditionalFields.size(), 2);
    assertEquals(schemaAdditionalFields.get("key1"), "value1");
    assertEquals(schemaAdditionalFields.get("key2"), "value2");

    // Reset original query and data schema properties
    SystemConfiguration.setProperty("data.schemas", dataSchemasProp);
    SystemConfiguration.setProperty("query.schemas", querySchemasProp);

    // Force the query and data schemas to load their original values
    if (!dataSchemasProp.equals("none"))
    {
      DataSchemaLoader.initialize();
    }

    if (!querySchemasProp.equals("none"))
    {
      QuerySchemaLoader.initialize();
    }

    logger.info("Finished testGeneralSchemaLoadWithAdditionalFields");
  }

  @Test
  public void testUnknownFilterClass() throws Exception
  {
    // Pull off the properties and reset upon completion
    String dataSchemasProp = SystemConfiguration.getProperty("data.schemas", "none");
    String querySchemasProp = SystemConfiguration.getProperty("query.schemas", "none");

    // Create the data schema used and force it to load
    createDataSchema("dataSchemaFile");
    DataSchemaLoader.initialize();

    // Create the query schema used and force it to load
    TestUtils.createQuerySchema(querySchemaFile, querySchemaName, dataSchemaName, "nonExistentElement", queryElements, filterElements, "bogusFilterClass");

    try
    {
      QuerySchemaLoader.initialize();
      fail("QuerySchemaLoader did not throw exception for bogus filter class");
    } catch (PIRException ignore)
    {
      // Expected
    }

    // Reset original query and data schema properties
    SystemConfiguration.setProperty("data.schemas", dataSchemasProp);
    SystemConfiguration.setProperty("query.schemas", querySchemasProp);

    // Force the query and data schemas to load their original values
    if (!dataSchemasProp.equals("none"))
    {
      DataSchemaLoader.initialize();
    }

    if (!querySchemasProp.equals("none"))
    {
      QuerySchemaLoader.initialize();
    }

    logger.info("Finished testFunkyFilterScenarios");
  }

  @Test
  public void testDataSchemaDoesNotExist() throws Exception
  {
    logger.info("Starting testDataSchemaDoesNotExist: ");

    // Pull off the properties and reset upon completion
    String querySchemasProp = SystemConfiguration.getProperty("query.schemas", "none");

    // Create the query schema used and force it to load
    TestUtils.createQuerySchema(querySchemaFile, querySchemaName, dataSchemaName + "bogus", element4, queryElements, filterElements, null);
    try
    {
      QuerySchemaLoader.initialize();
      fail("QuerySchemaLoader did not throw exception for non-existent DataSchema");
    } catch (PIRException ignore)
    {
      // Expected
    }

    // Reset original query properties and force to load
    SystemConfiguration.setProperty("query.schemas", querySchemasProp);
    if (!querySchemasProp.equals("none"))
    {
      QuerySchemaLoader.initialize();
    }

    logger.info("Finished testDataSchemaDoesNotExist ");
  }

  @Test
  public void testSelectorDoesNotExistInDataSchema() throws Exception
  {
    logger.info("Starting testSelectorDoesNotExistInDataSchema: ");

    // Pull off the properties and reset upon completion
    String dataSchemasProp = SystemConfiguration.getProperty("data.schemas", "none");
    String querySchemasProp = SystemConfiguration.getProperty("query.schemas", "none");

    // Create the data schema used and force it to load
    createDataSchema("dataSchemaFile");
    DataSchemaLoader.initialize();

    // Create the query schema used and force it to load
    TestUtils.createQuerySchema(querySchemaFile, querySchemaName, dataSchemaName, "nonExistentElement", queryElements, filterElements,
        StopListFilter.class.getName());

    try
    {
      QuerySchemaLoader.initialize();
      fail("QuerySchemaLoader did not throw exception for non-existent selectorName");
    } catch (Exception ignore)
    {
      // Expected
    }

    // Reset original query and data schema properties
    SystemConfiguration.setProperty("data.schemas", dataSchemasProp);
    SystemConfiguration.setProperty("query.schemas", querySchemasProp);

    // Force the query and data schemas to load their original values
    if (!dataSchemasProp.equals("none"))
    {
      DataSchemaLoader.initialize();
    }

    if (!querySchemasProp.equals("none"))
    {
      QuerySchemaLoader.initialize();
    }

    logger.info("Finished testSelectorDoesNotExistInDataSchema ");
  }

  // Create the stoplist file and alter the properties accordingly
  private void createStopListFile() throws IOException, PIRException
  {
    String newSLFile = Inputs.createStopList(null, false);

    SystemConfiguration.setProperty("pir.stopListFile", newSLFile);
  }

  // Create the test data schema file
  private void createDataSchema(String schemaFile) throws Exception
  {
    // Create a temporary file for the test schema, set in the properties
    File file = File.createTempFile(schemaFile, ".xml");
    file.deleteOnExit();
    logger.info("file = " + file.toString());
    SystemConfiguration.setProperty("data.schemas", file.toString());

    // Write to the file
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.newDocument();

    // root element
    Element rootElement = doc.createElement("schema");
    doc.appendChild(rootElement);

    // Add the schemaName
    Element schemaNameElement = doc.createElement("schemaName");
    schemaNameElement.appendChild(doc.createTextNode(dataSchemaName));
    rootElement.appendChild(schemaNameElement);

    // Add the elements
    // element1 -- single String
    TestUtils.addElement(doc, rootElement, element1, PrimitiveTypePartitioner.STRING, "false", PrimitiveTypePartitioner.class.getName());

    // element2 - -- array of Integers
    TestUtils.addElement(doc, rootElement, element2, PrimitiveTypePartitioner.INT, "true", PrimitiveTypePartitioner.class.getName());

    // element3 -- array of IP addresses
    TestUtils.addElement(doc, rootElement, element3, PrimitiveTypePartitioner.STRING, "true", IPDataPartitioner.class.getName());

    // element4 -- single byte type
    TestUtils.addElement(doc, rootElement, element4, PrimitiveTypePartitioner.BYTE, "false", PrimitiveTypePartitioner.class.getName());

    // Write to a xml file
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = transformerFactory.newTransformer();
    DOMSource source = new DOMSource(doc);
    StreamResult result = new StreamResult(file);
    transformer.transform(source, result);

    // Output for testing
    StreamResult consoleResult = new StreamResult(System.out);
    transformer.transform(source, consoleResult);
    System.out.println();
  }
}
