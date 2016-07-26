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
package test.schema.data;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.data.partitioner.IPDataPartitioner;
import org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner;
import org.apache.pirk.test.utils.TestUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test suite for LoadDataSchema and DataSchema
 */
public class LoadDataSchemaTest
{
  private static Logger logger = LoggerFactory.getLogger(LoadDataSchemaTest.class);

  private String dataSchemaName = "fakeDataSchema";

  private String element1 = "elementName1";
  private String element2 = "elementName2";
  private String element3 = "elementName3";

  @Test
  public void testGeneralSchemaLoad() throws Exception
  {
    // Pull off the property and reset upon completion
    String schemasProp = SystemConfiguration.getProperty("data.schemas", "none");

    // Write the schema file
    try
    {
      createDataSchema("schemaFile");
    } catch (IOException e)
    {
      e.printStackTrace();
      fail(e.toString());
    }

    // Force the schema to load
    DataSchemaLoader.initialize();

    // Check the entries
    DataSchema dSchema = DataSchemaRegistry.get(dataSchemaName);

    assertEquals(dataSchemaName, dSchema.getSchemaName());

    assertEquals(3, dSchema.getElementNames().size());
    
    // TODO: check Hadoop text names

    assertEquals(PrimitiveTypePartitioner.STRING, dSchema.getElementType(element1));
    assertEquals(PrimitiveTypePartitioner.INT, dSchema.getElementType(element2));
    assertEquals(PrimitiveTypePartitioner.STRING, dSchema.getElementType(element3));

    assertEquals(PrimitiveTypePartitioner.class.getName(), dSchema.getPartitionerTypeName(element1));
    if (!(dSchema.getPartitionerForElement(element1) instanceof PrimitiveTypePartitioner))
    {
      fail("Partitioner instance for element1 must be PrimitiveTypePartitioner");
    }
    assertEquals(IPDataPartitioner.class.getName(), dSchema.getPartitionerTypeName(element3));
    if (!(dSchema.getPartitionerForElement(element3) instanceof IPDataPartitioner))
    {
      fail("Partitioner instance for element3 must be IPDataPartitioner");
    }

    assertEquals(2, dSchema.getArrayElements().size());
    assertTrue(dSchema.getArrayElements().contains(element2));
    assertTrue(dSchema.getArrayElements().contains(element3));

    assertEquals(1, dSchema.getNonArrayElements().size());
    assertTrue(dSchema.getNonArrayElements().contains(element1));

    // Reset original data.schemas property
    SystemConfiguration.setProperty("data.schemas", schemasProp);

    // Force the schema to load
    if (!schemasProp.equals("none"))
    {
      DataSchemaLoader.initialize();
    }
  }

  @Test
  public void testIncorrectJavaType() throws Exception
  {
    // Pull off the property and reset upon completion
    String schemasProp = SystemConfiguration.getProperty("data.schemas");

    // Write the schema file
    try
    {
      createDataSchemaIncorrectJavaType("wrongJavaType");
    } catch (IOException e)
    {
      e.printStackTrace();
      fail(e.toString());
    }

    try
    {
      // Force the schema to load
      DataSchemaLoader.initialize();
      fail("DataSchemaLoader did not throw exception for incorrect javaType");
    } catch (Exception ignore)
    {}

    // Reset original data.schemas property
    SystemConfiguration.setProperty("data.schemas", schemasProp);

    // Force the schema to load
    DataSchemaLoader.initialize();
  }

  @Test
  public void testUnknownPartitioner() throws Exception
  {
    // Pull off the property and reset upon completion
    String schemasProp = SystemConfiguration.getProperty("data.schemas");

    // Write the schema file
    try
    {
      createDataSchemaUnknownPartitioner("unknownPartitioner");
    } catch (IOException e)
    {
      e.printStackTrace();
      fail(e.toString());
    }

    try
    {
      // Force the schema to load
      DataSchemaLoader.initialize();
      fail("DataSchemaLoader did not throw exception for unknown partitioner");
    } catch (Exception ignore)
    {}

    // Reset original data.schemas property
    SystemConfiguration.setProperty("data.schemas", schemasProp);

    // Force the schema to load
    DataSchemaLoader.initialize();
  }

  // Create the file that contains an unknown partitioner
  private void createDataSchemaUnknownPartitioner(String schemaFile) throws IOException
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

      // Add the element - unknown partitioner
      TestUtils.addElement(doc, rootElement, element1, PrimitiveTypePartitioner.INT, "false", "fakePartitioner");

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
      // TestUtils.addElement(doc, rootElement, element1, PrimitiveTypePartitioner.STRING, "false", PrimitiveTypePartitioner.class.getName());
      TestUtils.addElement(doc, rootElement, element1, PrimitiveTypePartitioner.STRING, "false", null);

      // element2 - -- array of Integers
      TestUtils.addElement(doc, rootElement, element2, PrimitiveTypePartitioner.INT, "true", PrimitiveTypePartitioner.class.getName());

      // element3 -- array of IP addresses
      TestUtils.addElement(doc, rootElement, element3, PrimitiveTypePartitioner.STRING, "true", IPDataPartitioner.class.getName());

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

  // Create the test schema file
  private void createDataSchemaIncorrectJavaType(String schemaFile) throws IOException
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

      // Add the element - unknown Java type
      TestUtils.addElement(doc, rootElement, element1, "bogus", "false", PrimitiveTypePartitioner.class.getName());

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
