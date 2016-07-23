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
package org.apache.pirk.schema.data;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.pirk.schema.data.partitioner.DataPartitioner;
import org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Class to load any data schemas specified in the properties file, 'data.schemas'
 * <p>
 * Schemas should be specified as follows; all items are treated in a case insensitive manner:
 * 
 * <pre>
 * {@code
 * <schema>
 *  <schemaName> name of the schema </schemaName>
 *  <element>
 *      <name> element name /name>
 *      <type> class name or type name (if Java primitive type) of the element </type>
 *      <isArray> true or false -- whether or not the schema element is an array within the data </isArray>
 *      <partitioner> optional - Partitioner class for the element; defaults to primitive java type partitioner </partitioner> 
 *  </element>
 * </schema>
 * }
 * </pre>
 * 
 * Primitive types must be one of the following: "byte", "short", "int", "long", "float", "double", "char", "string"
 * <p>
 */
public class LoadDataSchemas
{
  private static final Logger logger = LoggerFactory.getLogger(LoadDataSchemas.class);

  private static HashMap<String,DataSchema> schemaMap;

  private static HashSet<String> allowedPrimitiveJavaTypes = new HashSet<>(Arrays.asList(PrimitiveTypePartitioner.BYTE, PrimitiveTypePartitioner.SHORT,
      PrimitiveTypePartitioner.INT, PrimitiveTypePartitioner.LONG, PrimitiveTypePartitioner.FLOAT, PrimitiveTypePartitioner.DOUBLE,
      PrimitiveTypePartitioner.CHAR, PrimitiveTypePartitioner.STRING));

  static
  {
    logger.info("Loading data schemas: ");

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
    String dataSchemas = SystemConfiguration.getProperty("data.schemas", "none");
    if (!dataSchemas.equals("none"))
    {
      String[] dataSchemaFiles = dataSchemas.split(",");
      for (String schemaFile : dataSchemaFiles)
      {
        logger.info("Loading schemaFile = " + schemaFile + " hdfs = " + hdfs);

        // Parse and load the schema file into a DataSchema object; place in the schemaMap
        DataSchema dataSchema = loadDataSchemaFile(schemaFile, hdfs, fs);
        schemaMap.put(dataSchema.getSchemaName(), dataSchema);
      }
    }
  }

  public static HashMap<String,DataSchema> getSchemaMap()
  {
    return schemaMap;
  }

  public static Set<String> getSchemaNames()
  {
    return schemaMap.keySet();
  }

  public static DataSchema getSchema(String schemaName)
  {
    return schemaMap.get(schemaName.toLowerCase());
  }

  private static DataSchema loadDataSchemaFile(String schemaFile, boolean hdfs, FileSystem fs) throws Exception
  {
    DataSchema dataSchema;

    // Initialize the elements needed to create the DataSchema
    String schemaName;
    HashMap<String,Text> textRep = new HashMap<>();
    HashSet<String> listRep = new HashSet<>();
    HashMap<String,String> typeMap = new HashMap<>();
    HashMap<String,String> partitionerMap = new HashMap<>();
    HashMap<String,Object> partitionerInstances = new HashMap<>();

    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

    // Read in and parse the schema file
    Document doc;
    if (hdfs)
    {
      Path filePath = new Path(schemaFile);
      logger.info("hdfs: filePath = " + filePath.toString());
      doc = dBuilder.parse(fs.open(filePath));
    }
    else
    {
      File inputFile = new File(schemaFile);
      logger.info("localFS: inputFile = " + inputFile.toString());
      doc = dBuilder.parse(inputFile);
    }
    doc.getDocumentElement().normalize();
    logger.info("Root element: " + doc.getDocumentElement().getNodeName());

    logger.info("Root element: " + doc.getDocumentElement().getNodeName());

    // Extract the schemaName
    NodeList schemaNameList = doc.getElementsByTagName("schemaName");
    if (schemaNameList.getLength() > 1)
    {
      throw new Exception("schemaNameList.getLength() = " + schemaNameList.getLength() + " -- should be 1, " + "one schema per xml file");
    }
    schemaName = schemaNameList.item(0).getTextContent().trim().toLowerCase();
    logger.info("schemaName = " + schemaName);

    // Extract the elements
    NodeList nList = doc.getElementsByTagName("element");
    for (int i = 0; i < nList.getLength(); i++)
    {
      Node nNode = nList.item(i);
      if (nNode.getNodeType() == Node.ELEMENT_NODE)
      {
        Element eElement = (Element) nNode;

        // Pull out the attributes
        String name = eElement.getElementsByTagName("name").item(0).getTextContent().trim().toLowerCase();
        String type = eElement.getElementsByTagName("type").item(0).getTextContent().trim();

        // An absent isArray means false, and an empty isArray means true, otherwise take the value. 
        String isArray = "false";
        Node isArrayNode = eElement.getElementsByTagName("isArray").item(0);
        if (isArrayNode != null)
        {
          String isArrayValue = isArrayNode.getTextContent().trim().toLowerCase();
          isArray = isArrayValue.isEmpty() ? "true" : isArrayValue;
        }

        // Pull and check the partitioner class -- if the partitioner tag doesn't exist, then
        // it defaults to the PrimitiveTypePartitioner
        String partitioner = null;
        boolean primitivePartitioner = true;
        if (eElement.getElementsByTagName("partitioner").item(0) != null)
        {
          partitioner = eElement.getElementsByTagName("partitioner").item(0).getTextContent().trim();
          if (!partitioner.equals(PrimitiveTypePartitioner.class.getName()))
          {
            primitivePartitioner = false;
          }
        }
        if (primitivePartitioner)
        {
          partitioner = PrimitiveTypePartitioner.class.getName();
          partitionerInstances.put(PrimitiveTypePartitioner.class.getName(), new PrimitiveTypePartitioner());

          // Validate the type since we are using the primitive partioner
          if (!allowedPrimitiveJavaTypes.contains(type.toLowerCase()))
          {
            throw new Exception("javaType = " + type + " is not one of the allowed javaTypes: " + " byte, short, int, long, float, double, char, string");
          }
        }
        else
        // If we have a non-primitive partitioner
        {
          Class c = Class.forName(partitioner);
          Object obj = c.newInstance();
          if (!(obj instanceof DataPartitioner))
          {
            throw new Exception("partitioner = " + partitioner + " DOES NOT implement the DataPartitioner interface");
          }
          partitionerInstances.put(partitioner, obj);
        }

        // Place in the appropriate structures
        textRep.put(name, new Text(name));
        typeMap.put(name, type);
        partitionerMap.put(name, partitioner);

        if (isArray.equals("true"))
        {
          listRep.add(name);
        }

        logger.info("name = " + name + " javaType = " + type + " isArray = " + isArray + " partitioner " + partitioner);
      }
    }

    // Construct the DataSchema and set the partitionerInstances
    dataSchema = new DataSchema(schemaName, textRep, listRep, typeMap, partitionerMap);
    dataSchema.setPartitionerInstances(partitionerInstances);

    return dataSchema;
  }
}
