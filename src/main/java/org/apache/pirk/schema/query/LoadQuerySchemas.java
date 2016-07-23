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

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.LoadDataSchemas;
import org.apache.pirk.schema.data.partitioner.DataPartitioner;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Class to load any query schemas specified in the properties file, 'query.schemas'
 * <p>
 * Schemas should be specified as follows; all items are treated in a case insensitive manner:
 * 
 * <pre>
 * {@code
 *  <schema>
 *    <schemaName> name of the schema </schemaName>
 *    <dataSchemaName> name of the data schema over which this query is run </dataSchemaName>
 *    <selectorName> name of the element in the data schema that will be the selector </selectorName>
 *    <elements>
 *       <name> element name of element in the data schema to include in the query response </name>
 *    </elements>
 *    <filter> (optional) name of the filter class to use to filter the data </filter>
 *    <filterNames> (optional)
 *       <name> element name of element in the data schema to apply pre-processing filters </name>
 *    </filterNames>
 *   </schema>
 * }
 * </pre>
 * <p>
 * TODO: Allow the schema to specify how many array elements to return per element, if the element is an array type
 */
public class LoadQuerySchemas
{
  private static final Logger logger = LoggerFactory.getLogger(LoadQuerySchemas.class);

  private static HashMap<String,QuerySchema> schemaMap;

  static
  {
    logger.info("Loading query schemas: ");

    schemaMap = new HashMap<>();
    try
    {
      initialize();
    } catch (Exception e)
    {
      logger.error("Caught exception: ");
      e.printStackTrace();
    }
  }

  public static void initialize() throws Exception
  {
    initialize(false, null);
  }

  public static void initialize(boolean hdfs, FileSystem fs) throws Exception
  {
    String querySchemas = SystemConfiguration.getProperty("query.schemas", "none");
    if (!querySchemas.equals("none"))
    {
      String[] querySchemaFiles = querySchemas.split(",");
      for (String schemaFile : querySchemaFiles)
      {
        logger.info("Loading schemaFile = " + schemaFile);

        // Parse and load the schema file into a QuerySchema object; place in the schemaMap
        QuerySchema querySchema = loadQuerySchemaFile(schemaFile, hdfs, fs);
        schemaMap.put(querySchema.getSchemaName(), querySchema);
      }
    }
  }

  public static HashMap<String,QuerySchema> getSchemaMap()
  {
    return schemaMap;
  }

  public static Set<String> getSchemaNames()
  {
    return schemaMap.keySet();
  }

  public static QuerySchema getSchema(String schemaName)
  {
    return schemaMap.get(schemaName.toLowerCase());
  }

  private static QuerySchema loadQuerySchemaFile(String schemaFile, boolean hdfs, FileSystem fs) throws Exception
  {
    QuerySchema querySchema;

    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

    // Read in and parse the schema file
    Document doc;
    if (hdfs)
    {
      Path filePath = new Path(schemaFile);
      doc = dBuilder.parse(fs.open(filePath));
      logger.info("hdfs: filePath = " + filePath.toString());
    }
    else
    {
      File inputFile = new File(schemaFile);
      doc = dBuilder.parse(inputFile);
      logger.info("localFS: inputFile = " + inputFile.toString());
    }
    doc.getDocumentElement().normalize();
    logger.info("Root element: " + doc.getDocumentElement().getNodeName());

    // Extract the schemaName
    String schemaName = extractValue(doc, "schemaName");
    logger.info("schemaName = " + schemaName);

    // Extract the dataSchemaName
    String dataSchemaName = extractValue(doc, "dataSchemaName");
    logger.info("dataSchemaName = " + dataSchemaName);

    DataSchema dataSchema = LoadDataSchemas.getSchema(dataSchemaName);
    if (dataSchema == null)
    {
      throw new Exception("Loaded DataSchema does not exist for dataSchemaName = " + dataSchemaName);
    }

    // Extract the selectorName
    String selectorName = extractValue(doc, "selectorName");
    logger.info("selectorName = " + selectorName);
    if (!dataSchema.containsElement(selectorName))
    {
      throw new Exception("dataSchema = " + dataSchemaName + " does not contain selectorName = " + selectorName);
    }

    // Extract the elements
    NodeList elementsList = doc.getElementsByTagName("elements");
    if (elementsList.getLength() > 1)
    {
      throw new Exception("elementsList.getLength() = " + elementsList.getLength() + " -- should be 1");
    }
    Element elements = (Element) elementsList.item(0);

    TreeSet<String> elementNames = new TreeSet<>();
    int dataElementSize = 0;
    NodeList nList = elements.getElementsByTagName("name");
    for (int i = 0; i < nList.getLength(); i++)
    {
      Node nNode = nList.item(i);
      if (nNode.getNodeType() == Node.ELEMENT_NODE)
      {
        Element eElement = (Element) nNode;

        // Pull the name and add to the TreeSet
        String name = eElement.getFirstChild().getNodeValue().trim().toLowerCase();
        elementNames.add(name);

        // Compute the number of bits for this element
        logger.info("name = " + name);
        logger.info("partitionerName = " + dataSchema.getPartitionerName(name));
        if ((dataSchema.getPartitionerForElement(name)) == null)
        {
          logger.info("partitioner is null");
        }
        int bits = ((DataPartitioner) dataSchema.getPartitionerForElement(name)).getBits(dataSchema.getElementType(name));

        // Multiply by the number of array elements allowed, if applicable
        if (dataSchema.getListRep().contains(name))
        {
          bits *= Integer.parseInt(SystemConfiguration.getProperty("pir.numReturnArrayElements"));
        }
        dataElementSize += bits;

        logger.info("name = " + name + " bits = " + bits + " dataElementSize = " + dataElementSize);
      }
    }

    // Extract the filter, if it exists
    String filter = QuerySchema.NO_FILTER;
    if (doc.getElementsByTagName("filter").item(0) != null)
    {
      filter = doc.getElementsByTagName("filter").item(0).getTextContent().trim();
    }

    // Extract the filterNames, if they exist
    HashSet<String> filterNamesSet = new HashSet<>();
    if (doc.getElementsByTagName("filterNames").item(0) != null)
    {
      NodeList filterNamesList = doc.getElementsByTagName("filterNames");
      if (filterNamesList.getLength() > 1)
      {
        throw new Exception("filterNamesList.getLength() = " + filterNamesList.getLength() + " -- should be 1");
      }
      Element filterNames = (Element) filterNamesList.item(0);

      NodeList filterNList = filterNames.getElementsByTagName("name");
      for (int i = 0; i < filterNList.getLength(); i++)
      {
        Node nNode = filterNList.item(i);
        if (nNode.getNodeType() == Node.ELEMENT_NODE)
        {
          Element eElement = (Element) nNode;

          // Pull the name and add to the TreeSet
          String name = eElement.getFirstChild().getNodeValue().trim().toLowerCase();
          filterNamesSet.add(name);

          logger.info("filterName = " + name);
        }
      }
    }

    // Create the query schema object
    querySchema = new QuerySchema(schemaName, dataSchemaName, elementNames, selectorName, dataElementSize, filterNamesSet, filter);

    return querySchema;
  }

  /**
   * Extracts a top level, single value from the xml structure
   */
  private static String extractValue(Document doc, String valueName) throws Exception
  {
    String value;

    NodeList itemList = doc.getElementsByTagName(valueName);
    if (itemList.getLength() > 1)
    {
      throw new Exception("itemList.getLength() = " + itemList.getLength() + " -- should be 1");
    }
    value = itemList.item(0).getTextContent().trim().toLowerCase();

    return value;
  }

  /**
   * Checks whether or not (true/false) the given schema is loaded
   */
  public static boolean containsSchema(String schemaNameIn)
  {
    return schemaMap.containsKey(schemaNameIn);
  }

  public static void printSchemas()
  {
    for (String schema : schemaMap.keySet())
    {
      logger.info("schema = " + schema);
    }
  }

  public static String getSchemasString()
  {
    String schemasString = "";
    for (String schema : schemaMap.keySet())
    {
      schemasString += " \n" + schema;
    }
    return schemasString;
  }
}
