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
package test.schema.query;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.pirk.schema.data.LoadDataSchemas;
import org.apache.pirk.schema.data.partitioner.IPDataPartitioner;
import org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.filter.StopListFilter;
import org.apache.pirk.test.utils.Inputs;
import org.apache.pirk.test.utils.TestUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import test.schema.data.LoadDataSchemaTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import test.schema.data.LoadDataSchemaTest;

/**
 * Test suite for LoadQuerySchema and QuerySchema
 */
public class LoadQuerySchemaTest
{
  private static final Logger logger = LoggerFactory.getLogger(LoadDataSchemaTest.class);

  private String querySchemaFile = "querySchemaFile";
  private String dataSchemaName = "fakeDataSchema";
  private String querySchemaName = "fakeQuerySchema";

  private String element1 = "elementName1";
  private String element2 = "elementName2";
  private String element3 = "elementName3";
  private String element4 = "elementName4";

  private List<String> queryElements = Arrays.asList(element1, element2, element3);
  private List<String> filterElements = Collections.singletonList(element2);

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
    try
    {
      createDataSchema("dataSchemaFile");
    } catch (Exception e)
    {
      e.printStackTrace();
      fail(e.toString());
    }
    LoadDataSchemas.initialize();

    // Create the query schema used and force it to load
    try
    {
      TestUtils.createQuerySchema(querySchemaFile, querySchemaName, dataSchemaName, element4, queryElements, filterElements, StopListFilter.class.getName());

    } catch (IOException e)
    {
      e.printStackTrace();
      fail(e.toString());
    }
    LoadQuerySchemas.initialize();

    // Check the entries
    QuerySchema qSchema = LoadQuerySchemas.getSchema(querySchemaName);

    assertEquals(querySchemaName.toLowerCase(), qSchema.getSchemaName());
    assertEquals(dataSchemaName.toLowerCase(), qSchema.getDataSchemaName());
    assertEquals(element4.toLowerCase(), qSchema.getSelectorName());

    assertEquals(StopListFilter.class.getName(), qSchema.getFilter());
    if (!(qSchema.getFilterInstance() instanceof StopListFilter))
    {
      fail("Filter class instance must be StopListFilter");
    }

    assertEquals(3, qSchema.getElementNames().size());
    for (String item : qSchema.getElementNames())
    {
      if (!(item.equals(element1.toLowerCase()) || item.equals(element2.toLowerCase()) || item.equals(element3.toLowerCase())))
      {
        fail("elementNames: item = " + item + " must equal one of: " + element1.toLowerCase() + ", " + element2.toLowerCase() + ", or "
            + element3.toLowerCase());
      }
    }
    assertEquals(1, qSchema.getFilterElementNames().size());
    for (String item : qSchema.getFilterElementNames())
    {
      if (!item.equals(element2.toLowerCase()))
      {
        fail("filterElementNames: item = " + item + " must equal " + element2.toLowerCase());
      }
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
      LoadDataSchemas.initialize();
    }

    if (!querySchemasProp.equals("none"))
    {
      LoadQuerySchemas.initialize();
    }

    logger.info("Finished testGeneralSchemaLoad: ");
  }

  @Test
  public void testUnknownFilterClass() throws Exception
  {
    // Pull off the properties and reset upon completion
    String dataSchemasProp = SystemConfiguration.getProperty("data.schemas", "none");
    String querySchemasProp = SystemConfiguration.getProperty("query.schemas", "none");

    // Create the data schema used and force it to load
    try
    {
      createDataSchema("dataSchemaFile");
    } catch (Exception e)
    {
      e.printStackTrace();
      fail(e.toString());
    }
    LoadDataSchemas.initialize();

    // Create the query schema used and force it to load
    try
    {
      TestUtils.createQuerySchema(querySchemaFile, querySchemaName, dataSchemaName, "nonExistentElement", queryElements, filterElements, "bogusFilterClass");

    } catch (IOException e)
    {
      e.printStackTrace();
      fail(e.toString());
    }
    try
    {
      LoadQuerySchemas.initialize();
      fail("LoadQuerySchemas did not throw exception for bogus filter class");
    } catch (Exception ignore)
    {}

    // Reset original query and data schema properties
    SystemConfiguration.setProperty("data.schemas", dataSchemasProp);
    SystemConfiguration.setProperty("query.schemas", querySchemasProp);

    // Force the query and data schemas to load their original values
    if (!dataSchemasProp.equals("none"))
    {
      LoadDataSchemas.initialize();
    }

    if (!querySchemasProp.equals("none"))
    {
      LoadQuerySchemas.initialize();
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
    try
    {
      TestUtils.createQuerySchema(querySchemaFile, querySchemaName, dataSchemaName, element4, queryElements, filterElements, null);

    } catch (IOException e)
    {
      e.printStackTrace();
      fail(e.toString());
    }
    try
    {
      LoadQuerySchemas.initialize();
      fail("LoadQuerySchemas did not throw exception for non-existent DataSchema");
    } catch (Exception ignore)
    {}

    // Reset original query properties and force to load
    SystemConfiguration.setProperty("query.schemas", querySchemasProp);
    if (!querySchemasProp.equals("none"))
    {
      LoadQuerySchemas.initialize();
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
    try
    {
      createDataSchema("dataSchemaFile");
    } catch (Exception e)
    {
      e.printStackTrace();
      fail(e.toString());
    }
    LoadDataSchemas.initialize();

    // Create the query schema used and force it to load
    try
    {
      TestUtils.createQuerySchema(querySchemaFile, querySchemaName, dataSchemaName, "nonExistentElement", queryElements, filterElements,
          StopListFilter.class.getName());

    } catch (IOException e)
    {
      e.printStackTrace();
      fail(e.toString());
    }
    try
    {
      LoadQuerySchemas.initialize();
      fail("LoadQuerySchemas did not throw exception for non-existent selectorName");
    } catch (Exception ignore)
    {}

    // Reset original query and data schema properties
    SystemConfiguration.setProperty("data.schemas", dataSchemasProp);
    SystemConfiguration.setProperty("query.schemas", querySchemasProp);

    // Force the query and data schemas to load their original values
    if (!dataSchemasProp.equals("none"))
    {
      LoadDataSchemas.initialize();
    }

    if (!querySchemasProp.equals("none"))
    {
      LoadQuerySchemas.initialize();
    }

    logger.info("Finished testSelectorDoesNotExistInDataSchema ");
  }

  // Create the stoplist file and alter the properties accordingly
  private void createStopListFile() throws IOException
  {
    SystemConfiguration.setProperty("pir.stopListFile", "testStopListFile");
    String newSLFile = Inputs.createPIRStopList(null, false);
    SystemConfiguration.setProperty("pir.stopListFile", newSLFile);
  }

  // Create the test data schema file
  private void createDataSchema(String schemaFile) throws IOException
  {
    // Create a temporary file for the test schema, set in the properties
    File file = File.createTempFile(schemaFile, ".xml");
    file.deleteOnExit();
    logger.info("file = " + file.toString());
    SystemConfiguration.setProperty("data.schemas", file.toString());

    // Write to the file
    try
    {
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

    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
