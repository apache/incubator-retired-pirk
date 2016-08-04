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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.data.partitioner.DataPartitioner;
import org.apache.pirk.schema.query.filter.DataFilter;
import org.apache.pirk.schema.query.filter.FilterFactory;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Class to load any query schemas specified in the properties file, 'query.schemas'
 * <p>
 * Schemas should be specified as follows:
 *
 * <pre>{@code
 * <schema>
 *    <schemaName> name of the schema </schemaName>
 *    <dataSchemaName> name of the data schema over which this query is run </dataSchemaName>
 *    <selectorName> name of the element in the data schema that will be the selector </selectorName>
 *    <elements>
 *       <name> element name of element in the data schema to include in the query response; just
 *              as with the data schema, the element name is case sensitive</name>
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
public class QuerySchemaLoader
{
  private static final Logger logger = LoggerFactory.getLogger(QuerySchemaLoader.class);

  private static final String NO_FILTER = "noFilter";

  static
  {
    logger.info("Loading query schemas: ");

    try
    {
      initialize();
    } catch (Exception e)
    {
      logger.error("Caught exception: ");
      e.printStackTrace();
    }
  }

  /* Kept for compatibility */
  /**
   * Initializes the static {@link QuerySchemaRegistry} with a list of
   * query schema names.
   * @throws Exception
   */
  public static void initialize() throws Exception
  {
    initialize(false, null);
  }

  /* Kept for compatibility */
  /**
   * Initializes the static {@link QuerySchemaRegistry} with a list of
   * available query schema names.
   * @param hdfs If true, specifies that the query schema is an hdfs file; if false, that it is a regular file.
   * @param fs Used only when {@paramref hdfs} is true; the {@link FileSystem} handle for the hdfs in which the query schema exists
   * @throws Exception
   */
  public static void initialize(boolean hdfs, FileSystem fs) throws Exception
  {
    String querySchemas = SystemConfiguration.getProperty("query.schemas", "none");
    if (querySchemas.equals("none"))
    {
      return;
    }
    String[] querySchemaFiles = querySchemas.split(",");
    for (String schemaFile : querySchemaFiles)
    {
      logger.info("Loading schemaFile = " + schemaFile);

      // Parse and load the schema file into a QuerySchema object; place in the schemaMap
      QuerySchemaLoader loader = new QuerySchemaLoader();
      InputStream is;
      if (hdfs)
      {
        is = fs.open(new Path(schemaFile));
        logger.info("hdfs: filePath = " + schemaFile);
      }
      else
      {
        is = new FileInputStream(schemaFile);
        logger.info("localFS: inputFile = " + schemaFile);
      }

      try
      {
        QuerySchema querySchema = loader.loadSchema(is);
        QuerySchemaRegistry.put(querySchema);
      } finally
      {
        is.close();
      }
    }
  }

  /**
   * Default constructor.
   */
  public QuerySchemaLoader()
  {

  }

  /**
   * Returns the query schema as defined in XML format on the given stream.
   * 
   * @param stream
   *          The source of the XML query schema description.
   * @return The query schema.
   * @throws IOException
   *           A problem occurred reading from the given stream.
   * @throws PIRException
   *           The schema description is invalid.
   */
  public QuerySchema loadSchema(InputStream stream) throws IOException, PIRException
  {
    // Read in and parse the XML file.
    Document doc = parseXMLDocument(stream);

    // Extract the schemaName.
    String schemaName = extractValue(doc, "schemaName");
    logger.info("schemaName = " + schemaName);

    // Extract the dataSchemaName.
    String dataSchemaName = extractValue(doc, "dataSchemaName");
    logger.info("dataSchemaName = " + dataSchemaName);

    // We must have a matching data schema for this query.
    DataSchema dataSchema = DataSchemaRegistry.get(dataSchemaName);
    if (dataSchema == null)
    {
      throw new PIRException("Loaded DataSchema does not exist for dataSchemaName = " + dataSchemaName);
    }

    // Extract the selectorName, and ensure it matches an element in the data schema.
    String selectorName = extractValue(doc, "selectorName");
    logger.info("selectorName = " + selectorName);
    if (!dataSchema.containsElement(selectorName))
    {
      throw new PIRException("dataSchema = " + dataSchemaName + " does not contain selectorName = " + selectorName);
    }

    // Extract the query elements.
    NodeList elementsList = doc.getElementsByTagName("elements");
    if (elementsList.getLength() != 1)
    {
      throw new PIRException("elementsList.getLength() = " + elementsList.getLength() + " -- should be 1");
    }
    Element elements = (Element) elementsList.item(0);

    LinkedHashSet<String> elementNames = new LinkedHashSet<>();
    int dataElementSize = 0;
    NodeList nList = elements.getElementsByTagName("name");
    for (int i = 0; i < nList.getLength(); i++)
    {
      Node nNode = nList.item(i);
      if (nNode.getNodeType() == Node.ELEMENT_NODE)
      {
        // Pull the name
        String queryElementName = ((Element) nNode).getFirstChild().getNodeValue().trim();
        if (!dataSchema.containsElement(queryElementName))
        {
          throw new PIRException("dataSchema = " + dataSchemaName + " does not contain requested element name = " + queryElementName);
        }
        elementNames.add(queryElementName);
        logger.info("name = " + queryElementName + " partitionerName = " + dataSchema.getPartitionerTypeName(queryElementName));

        // Compute the number of bits for this element.
        DataPartitioner partitioner = dataSchema.getPartitionerForElement(queryElementName);
        int bits = partitioner.getBits(dataSchema.getElementType(queryElementName));

        // Multiply by the number of array elements allowed, if applicable.
        if (dataSchema.isArrayElement(queryElementName))
        {
          bits *= Integer.parseInt(SystemConfiguration.getProperty("pir.numReturnArrayElements"));
        }
        dataElementSize += bits;

        logger.info("name = " + queryElementName + " bits = " + bits + " dataElementSize = " + dataElementSize);
      }
    }

    // Extract the filter, if it exists
    String filterTypeName = NO_FILTER;
    if (doc.getElementsByTagName("filter").item(0) != null)
    {
      filterTypeName = doc.getElementsByTagName("filter").item(0).getTextContent().trim();
    }

    // Create a filter over the query elements.
    Set<String> filteredNamesSet = extractFilteredElementNames(doc);
    DataFilter filter = instantiateFilter(filterTypeName, filteredNamesSet);

    // Create and return the query schema object.
    QuerySchema querySchema = new QuerySchema(schemaName, dataSchemaName, selectorName, filterTypeName, filter, dataElementSize);
    querySchema.getElementNames().addAll(elementNames);
    querySchema.getFilteredElementNames().addAll(filteredNamesSet);
    return querySchema;
  }

  /*
   * Parses and normalizes the XML document available on the given stream.
   * @param stream The input stream.
   * @return A {@link Document} representing the XML document.
   * @throws IOException
   * @throws PIRException
   */
  private Document parseXMLDocument(InputStream stream) throws IOException, PIRException
  {
    Document doc;
    try
    {
      DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      doc = dBuilder.parse(stream);
    } catch (ParserConfigurationException | SAXException e)
    {
      throw new PIRException("Schema parsing error", e);
    }
    doc.getDocumentElement().normalize();
    logger.info("Root element: " + doc.getDocumentElement().getNodeName());

    return doc;
  }

  /*
   * Returns the possibly empty set of element names over which the filter is applied, maintaining document order.
   *
   * @param doc
   * @return
   * @throws PIRException
   */
  private Set<String> extractFilteredElementNames(Document doc) throws PIRException
  {
    HashSet<String> filteredNamesSet = new HashSet<>();

    NodeList filterNamesList = doc.getElementsByTagName("filterNames");
    if (filterNamesList.getLength() != 0)
    {
      if (filterNamesList.getLength() > 1)
      {
        throw new PIRException("filterNamesList.getLength() = " + filterNamesList.getLength() + " -- should be 0 or 1");
      }

      // Extract element names from the list.
      Element foo = (Element) filterNamesList.item(0);
      NodeList filterNList = ((Element) filterNamesList.item(0)).getElementsByTagName("name");
      for (int i = 0; i < filterNList.getLength(); i++)
      {
        Node nNode = filterNList.item(i);
        if (nNode.getNodeType() == Node.ELEMENT_NODE)
        {
          // Pull the name and add to the set.
          String name = ((Element) nNode).getFirstChild().getNodeValue().trim();
          filteredNamesSet.add(name);

          logger.info("filterName = " + name);
        }
      }
    }
    return filteredNamesSet;
  }

  /*
   * Extracts a top level, single value from the XML structure.
   * 
   * Throws an exception if there is not exactly one tag with the given name.
   */
  private String extractValue(Document doc, String tagName) throws PIRException
  {
    NodeList itemList = doc.getElementsByTagName(tagName);
    if (itemList.getLength() != 1)
    {
      throw new PIRException("itemList.getLength() = " + itemList.getLength() + " -- should be 1");
    }
    return itemList.item(0).getTextContent().trim();
  }

  private DataFilter instantiateFilter(String filterTypeName, Set<String> filteredElementNames) throws IOException, PIRException
  {
    return filterTypeName.equals(NO_FILTER) ? null : FilterFactory.getFilter(filterTypeName, filteredElementNames);
  }
}
