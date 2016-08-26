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
package org.apache.pirk.test.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Class to hold testing utilities
 *
 */
public class TestUtils
{
  private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

  /**
   * Method to delete an ES index
   */
  public static void deleteESTestIndex(String index)
  {
    logger.info("Deleting index:");
    ProcessBuilder pDelete = new ProcessBuilder("curl", "-XDELETE", index);
    try
    {
      executeCommand(pDelete);
    } catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  /**
   * Method to execute process
   */
  public static void executeCommand(ProcessBuilder p) throws IOException
  {
    Process proc = p.start();

    try (BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream())))
    {
      // Read the output from the command
      logger.info("Standard output of the command:\n");
      String s;
      while ((s = stdInput.readLine()) != null)
      {
        logger.info(s);
      }

      // Read any errors from the attempted command
      logger.info("Standard error of the command (if any):\n");
      while ((s = stdError.readLine()) != null)
      {
        logger.info(s);
      }
    }
  }

  /**
   * Helper method to add elements to the test data schema
   */
  public static void addElement(Document doc, Element rootElement, String elementName, String typeIn, String isArrayIn, String partitionerIn)
  {
    Element element = doc.createElement("element");
    rootElement.appendChild(element);

    Element name = doc.createElement("name");
    name.appendChild(doc.createTextNode(elementName));
    element.appendChild(name);

    Element type = doc.createElement("type");
    type.appendChild(doc.createTextNode(typeIn));
    element.appendChild(type);

    if (isArrayIn.equals("true"))
    {
      element.appendChild(doc.createElement("isArray"));
    }

    if (partitionerIn != null)
    {
      Element partitioner = doc.createElement("partitioner");
      partitioner.appendChild(doc.createTextNode(partitionerIn));
      element.appendChild(partitioner);
    }
  }

  /**
   * Creates the test query schema file
   */
  public static void createQuerySchema(String schemaFile, String querySchemaName, String dataSchemaNameInput, String selectorNameInput,
      List<String> elementNames, List<String> filterNames, String filter) throws IOException
  {
    createQuerySchema(schemaFile, querySchemaName, dataSchemaNameInput, selectorNameInput, elementNames, filterNames, filter, true, null, false);
  }

  /**
   * Creates the test query schema file
   */
  public static void createQuerySchema(String schemaFile, String querySchemaName, String dataSchemaNameInput, String selectorNameInput,
      List<String> elementNames, List<String> filterNames, String filter, boolean append, FileSystem fs, boolean hdfs) throws IOException
  {
    logger.info("createQuerySchema: querySchemaName = " + querySchemaName);

    // Create a temporary file for the test schema, set in the properties
    String fileName;
    File file = null;
    OutputStreamWriter osw = null;
    if (hdfs)
    {
      Path filePath = new Path(schemaFile);
      fs.deleteOnExit(filePath);
      fileName = filePath.toString();

      osw = new OutputStreamWriter(fs.create(filePath, true));

      logger.info("hdfs: filePath = " + fileName);
    }
    else
    {
      file = File.createTempFile(schemaFile, ".xml");
      file.deleteOnExit();
      fileName = file.toString();
      logger.info("localFS: file = " + file.toString());
    }

    if (append)
    {
      String currentSchemas = SystemConfiguration.getProperty("query.schemas", "");
      if (currentSchemas.equals("") || currentSchemas.equals("none"))
      {
        SystemConfiguration.setProperty("query.schemas", fileName);
      }
      else
      {
        SystemConfiguration.setProperty("query.schemas", SystemConfiguration.getProperty("query.schemas", "") + "," + fileName);
      }
    }
    logger.info("query.schemas = " + SystemConfiguration.getProperty("query.schemas"));

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
      schemaNameElement.appendChild(doc.createTextNode(querySchemaName));
      rootElement.appendChild(schemaNameElement);

      // Add the dataSchemaName
      Element dataSchemaNameElement = doc.createElement("dataSchemaName");
      dataSchemaNameElement.appendChild(doc.createTextNode(dataSchemaNameInput));
      rootElement.appendChild(dataSchemaNameElement);

      // Add the selectorName
      Element selectorNameElement = doc.createElement("selectorName");
      selectorNameElement.appendChild(doc.createTextNode(selectorNameInput));
      rootElement.appendChild(selectorNameElement);

      // Add the elementNames
      Element elements = doc.createElement("elements");
      rootElement.appendChild(elements);
      for (String elementName : elementNames)
      {
        logger.info("elementName = " + elementName);
        Element name = doc.createElement("name");
        name.appendChild(doc.createTextNode(elementName));
        elements.appendChild(name);
      }

      // Add the filter
      if (filter != null)
      {
        Element filterElement = doc.createElement("filter");
        filterElement.appendChild(doc.createTextNode(filter));
        rootElement.appendChild(filterElement);

        // Add the filterNames
        Element filterNamesElement = doc.createElement("filterNames");
        rootElement.appendChild(filterNamesElement);
        for (String filterName : filterNames)
        {
          logger.info("filterName = " + filterName);
          Element name = doc.createElement("name");
          name.appendChild(doc.createTextNode(filterName));
          filterNamesElement.appendChild(name);
        }
      }

      // Write to a xml file
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      DOMSource source = new DOMSource(doc);
      StreamResult result;
      if (hdfs)
      {
        result = new StreamResult(osw);
      }
      else
      {
        result = new StreamResult(file);
      }
      transformer.transform(source, result);

      // Output for testing
      StreamResult consoleResult = new StreamResult(System.out);
      transformer.transform(source, consoleResult);
      System.out.println();

      if (osw != null)
      {
        osw.close();
      }

    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /**
   * Converts the result file into an ArrayList of QueryResponseJSON objects
   * @throws IOException 
   * @throws FileNotFoundException 
   */
  public static List<QueryResponseJSON> readResultsFile(File file) throws FileNotFoundException, IOException
  {
    List<QueryResponseJSON> results = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(file)))
    {
      String line;
      while ((line = br.readLine()) != null)
      {
        QueryResponseJSON jsonResult = new QueryResponseJSON(line);
        results.add(jsonResult);
      }
    }

    return results;
  }

  /**
   * Write the ArrayList<String to a tmp file in the local filesystem with the given fileName
   * 
   */
  public static String writeToTmpFile(List<String> list, String fileName, String suffix) throws IOException
  {
    File file = File.createTempFile(fileName, suffix);
    file.deleteOnExit();
    logger.info("localFS: file = " + file);

    FileWriter fw = new FileWriter(file);
    try (BufferedWriter bw = new BufferedWriter(fw))
    {
      for (String s : list)
      {
        bw.write(s);
        bw.newLine();
      }
    }

    return file.getPath();
  }
}
