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

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.data.partitioner.IPDataPartitioner;
import org.apache.pirk.schema.data.partitioner.ISO8601DatePartitioner;
import org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner;
import org.apache.pirk.schema.query.QuerySchemaLoader;
import org.apache.pirk.test.distributed.DistributedTestDriver;
import org.apache.pirk.utils.HDFS;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Input files for distributed testing
 *
 */
public class Inputs
{
  private static final Logger logger = LoggerFactory.getLogger(Inputs.class);

  // Test data schema fields
  public static final String DATE = "date";
  public static final String QNAME = "qname";
  public static final String SRCIP = "src_ip";
  public static final String DSTIP = "dest_ip";
  public static final String QTYPE = "qtype";
  public static final String RCODE = "rcode";
  public static final String IPS = "ip";

  // Test query types
  public static final String DNS_HOSTNAME_QUERY = "dns-hostname-query"; // Query for the watched hostnames occurred; ; watched value type -- hostname
  public static final String DNS_IP_QUERY = "dns-ip-query"; // The watched IP address(es) were detected in the response to a query; watched value type -- IP in
                                                            // IPS field (resolution IP)
  public static final String DNS_NXDOMAIN_QUERY = "dns-nxdomain-query"; // Query for nxdomain responses that were made for watched qnames
  public static final String DNS_SRCIP_QUERY = "dns-srcip-query"; // Query for responses from watched srcIPs
  public static final String DNS_SRCIP_QUERY_NO_FILTER = "dns-srcip-query-no-filter"; // Query for responses from watched srcIPs, no data filter used

  // Test query type files - localfs
  public static final String DNS_HOSTNAME_QUERY_FILE = DNS_HOSTNAME_QUERY + "_file";
  public static final String DNS_IP_QUERY_FILE = DNS_IP_QUERY + "_file";
  public static final String DNS_NXDOMAIN_QUERY_FILE = DNS_NXDOMAIN_QUERY + "_file";
  public static final String DNS_SRCIP_QUERY_FILE = DNS_SRCIP_QUERY + "_file";
  public static final String DNS_SRCIP_QUERY_NO_FILTER_FILE = DNS_SRCIP_QUERY_NO_FILTER + "_file";

  // Test query files hdfs
  public static final String DNS_HOSTNAME_QUERY_FILE_HDFS = "/tmp/" + DNS_HOSTNAME_QUERY + "_file";
  public static final String DNS_IP_QUERY_FILE_HDFS = "/tmp/" + DNS_IP_QUERY + "_file";
  public static final String DNS_NXDOMAIN_QUERY_FILE_HDFS = "/tmp/" + DNS_NXDOMAIN_QUERY + "_file";
  public static final String DNS_SRCIP_QUERY_FILE_HDFS = "/tmp/" + DNS_SRCIP_QUERY + "_file";
  public static final String DNS_SRCIP_QUERY_NO_FILTER_FILE_HDFS = "/tmp/" + DNS_SRCIP_QUERY_NO_FILTER + "_file";

  // Combined query file strings -- used to set properties
  public static final String LOCALFS_QUERY_FILES = DNS_HOSTNAME_QUERY_FILE + "," + DNS_IP_QUERY_FILE + "," + DNS_NXDOMAIN_QUERY_FILE + ","
      + DNS_SRCIP_QUERY_FILE + "," + DNS_SRCIP_QUERY_NO_FILTER_FILE;

  public static final String HDFS_QUERY_FILES = DNS_HOSTNAME_QUERY_FILE_HDFS + "," + DNS_IP_QUERY_FILE_HDFS + "," + DNS_NXDOMAIN_QUERY_FILE_HDFS + ","
      + DNS_SRCIP_QUERY_FILE_HDFS + "," + DNS_SRCIP_QUERY_NO_FILTER_FILE_HDFS;

  // Test data schema files -- localFS and hdfs
  public static final String TEST_DATA_SCHEMA_NAME = "testDataSchema";
  public static final String DATA_SCHEMA_FILE_LOCALFS = "testDataSchemaFile";
  public static final String DATA_SCHEMA_FILE_HDFS = "/tmp/testDataSchemaFile.xml";

  /**
   * Delete the ElasticSearch indices that was used for functional testing
   */
  public static void deleteESInput()
  {
    String esPIRIndex = SystemConfiguration.getProperty(DistributedTestDriver.ES_INPUT_NODES_PROPERTY) + ":"
        + SystemConfiguration.getProperty(DistributedTestDriver.ES_INPUT_PORT_PROPERTY) + "/"
        + SystemConfiguration.getProperty(DistributedTestDriver.ES_PIR_INPUT_INDEX_PROPERTY);
    logger.info("ES input being deleted at " + esPIRIndex);

    ProcessBuilder pDeletePIR = new ProcessBuilder("curl", "-XDELETE", esPIRIndex);
    try
    {
      TestUtils.executeCommand(pDeletePIR);
      logger.info("ES input deleted!");
    } catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  /**
   * Creates PIR JSON input
   */
  @SuppressWarnings("unchecked")
  public static ArrayList<JSONObject> createJSONDataElements()
  {
    ArrayList<JSONObject> dataElementsJSON = new ArrayList<>();

    JSONObject jsonObj1 = new JSONObject();
    jsonObj1.put(DATE, "2016-02-20T23:29:05.000Z");
    jsonObj1.put(QNAME, "a.b.c.com"); // hits on domain selector
    jsonObj1.put(SRCIP, "55.55.55.55"); // hits on IP selector
    jsonObj1.put(DSTIP, "1.2.3.6");
    jsonObj1.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj1.put(RCODE, 0);
    jsonObj1.put(IPS, new ArrayList<>(Arrays.asList("10.20.30.40", "10.20.30.60")));

    dataElementsJSON.add(jsonObj1);

    JSONObject jsonObj2 = new JSONObject();
    jsonObj2.put(DATE, "2016-02-20T23:29:06.000Z");
    jsonObj2.put(QNAME, "d.e.com");
    jsonObj2.put(SRCIP, "127.128.129.130");
    jsonObj2.put(DSTIP, "1.2.3.4");
    jsonObj2.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj2.put(RCODE, 0);
    jsonObj2.put(IPS, new ArrayList<>(Collections.singletonList("5.6.7.8")));

    dataElementsJSON.add(jsonObj2);

    JSONObject jsonObj3 = new JSONObject();
    jsonObj3.put(DATE, "2016-02-20T23:29:07.000Z");
    jsonObj3.put(QNAME, "d.e.com");
    jsonObj3.put(SRCIP, "131.132.133.134");
    jsonObj3.put(DSTIP, "9.10.11.12");
    jsonObj3.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj3.put(RCODE, 0);
    jsonObj3.put(IPS, new ArrayList<>(Collections.singletonList("13.14.15.16")));

    dataElementsJSON.add(jsonObj3);

    JSONObject jsonObj4 = new JSONObject();
    jsonObj4.put(DATE, "2016-02-20T23:29:08.000Z");
    jsonObj4.put(QNAME, "d.e.com");
    jsonObj4.put(SRCIP, "135.136.137.138");
    jsonObj4.put(DSTIP, "17.18.19.20");
    jsonObj4.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj4.put(RCODE, 3);
    jsonObj4.put(IPS, new ArrayList<>(Collections.singletonList("21.22.23.24")));

    dataElementsJSON.add(jsonObj4);

    JSONObject jsonObj5 = new JSONObject();
    jsonObj5.put(DATE, "2016-02-20T23:29:09.000Z");
    jsonObj5.put(QNAME, "d.e.com");
    jsonObj5.put(SRCIP, "139.140.141.142");
    jsonObj5.put(DSTIP, "25.26.27.28");
    jsonObj5.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj5.put(RCODE, 0);
    jsonObj5.put(IPS, new ArrayList<>(Collections.singletonList("5.6.7.8")));

    dataElementsJSON.add(jsonObj5);

    JSONObject jsonObj6 = new JSONObject();
    jsonObj6.put(DATE, "2016-02-20T23:29:10.000Z");
    jsonObj6.put(QNAME, "d.e.com");
    jsonObj6.put(SRCIP, "143.144.145.146");
    jsonObj6.put(DSTIP, "33.34.35.36");
    jsonObj6.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj6.put(RCODE, 0);
    jsonObj6.put(IPS, new ArrayList<>(Collections.singletonList("5.6.7.8")));

    dataElementsJSON.add(jsonObj6);

    JSONObject jsonObj7 = new JSONObject();
    jsonObj7.put(DATE, "2016-02-20T23:29:11.000Z");
    jsonObj7.put(QNAME, "something.else");
    jsonObj7.put(SRCIP, "1.1.1.1");
    jsonObj7.put(DSTIP, "2.2.2.2");
    jsonObj7.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj7.put(RCODE, 0);
    jsonObj7.put(IPS, new ArrayList<>(Collections.singletonList("3.3.3.3")));

    dataElementsJSON.add(jsonObj7);

    // This should never be returned - doesn't hit on any domain selectors
    // resolution ip on stoplist
    JSONObject jsonObj8 = new JSONObject();
    jsonObj8.put(DATE, "2016-02-20T23:29:12.000Z");
    jsonObj8.put(QNAME, "something.else2");
    jsonObj8.put(SRCIP, "5.6.7.8");
    jsonObj8.put(DSTIP, "2.2.2.22");
    jsonObj8.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj8.put(RCODE, 0);
    jsonObj8.put(IPS, new ArrayList<>(Collections.singletonList("3.3.3.132")));

    dataElementsJSON.add(jsonObj8);

    // This should never be returned in distributed case -- domain and resolution ip on stoplist
    JSONObject jsonObj9 = new JSONObject();
    jsonObj9.put(DATE, "2016-02-20T23:29:13.000Z");
    jsonObj9.put(QNAME, "something.else.on.stoplist");
    jsonObj9.put(SRCIP, "55.55.55.55");
    jsonObj9.put(DSTIP, "2.2.2.232");
    jsonObj9.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj9.put(RCODE, 0);
    jsonObj9.put(IPS, new ArrayList<>(Collections.singletonList("3.3.3.132")));

    dataElementsJSON.add(jsonObj9);

    return dataElementsJSON;
  }

  /**
   * Creates an ArrayList of JSONObjects with RCODE value of 3
   */
  @SuppressWarnings("unchecked")
  public static ArrayList<JSONObject> getRcode3JSONDataElements()
  {
    ArrayList<JSONObject> dataElementsJSON = new ArrayList<>();

    JSONObject jsonObj4 = new JSONObject();
    jsonObj4.put(DATE, "2016-02-20T23:29:08.000Z");
    jsonObj4.put(QNAME, "d.e.com");
    jsonObj4.put(SRCIP, "135.136.137.138");
    jsonObj4.put(DSTIP, "17.18.19.20");
    jsonObj4.put(QTYPE, new ArrayList<>(Collections.singletonList((short) 1)));
    jsonObj4.put(RCODE, 3);
    jsonObj4.put(IPS, new ArrayList<>(Collections.singletonList("21.22.23.24")));

    dataElementsJSON.add(jsonObj4);

    return dataElementsJSON;
  }

  /**
   * Creates PIR JSON input and writes to hdfs
   */
  @SuppressWarnings("unchecked")
  public static ArrayList<JSONObject> createPIRJSONInput(FileSystem fs)
  {
    String inputJSONFile = SystemConfiguration.getProperty(DistributedTestDriver.JSON_PIR_INPUT_FILE_PROPERTY);
    logger.info("PIR JSON input being created at " + inputJSONFile);

    ArrayList<JSONObject> dataElementsJSON = createJSONDataElements();

    HDFS.writeFile(dataElementsJSON, fs, inputJSONFile, true);
    logger.info("PIR JSON input successfully created!");

    return dataElementsJSON;
  }

  /**
   * Creates PIR Elasticsearch input
   */
  public static void createPIRESInput()
  {
    String esTestIndex = SystemConfiguration.getProperty(DistributedTestDriver.ES_INPUT_NODES_PROPERTY) + ":"
        + SystemConfiguration.getProperty(DistributedTestDriver.ES_INPUT_PORT_PROPERTY) + "/"
        + SystemConfiguration.getProperty(DistributedTestDriver.ES_PIR_INPUT_INDEX_PROPERTY);
    String esType = SystemConfiguration.getProperty(DistributedTestDriver.ES_INPUT_TYPE_PROPERTY);
    logger.info("ES input being created at " + esTestIndex + " with type " + esType);

    // Create ES Index
    logger.info("Creating new testindex:");
    ProcessBuilder pCreate = new ProcessBuilder("curl", "-XPUT", esTestIndex);
    try
    {
      TestUtils.executeCommand(pCreate);
    } catch (IOException e)
    {
      e.printStackTrace();
    }

    // Add elements
    logger.info(" \n \n Adding elements to testindex:");

    String indexTypeNum1 = esTestIndex + "/" + esType + "/1";
    logger.info("indexTypeNum1 = " + indexTypeNum1);
    ProcessBuilder pAdd1 = new ProcessBuilder("curl", "-XPUT", indexTypeNum1, "-d",
        "{\"qname\":\"a.b.c.com\",\"date\":\"2016-02-20T23:29:05.000Z\",\"qtype\":[\"1\"]"
            + ",\"rcode\":\"0\",\"src_ip\":\"55.55.55.55\",\"dest_ip\":\"1.2.3.6\"" + ",\"ip\":[\"10.20.30.40\",\"10.20.30.60\"]}");

    String indexTypeNum2 = esTestIndex + "/" + esType + "/2";
    logger.info("indexTypeNum2 = " + indexTypeNum2);
    ProcessBuilder pAdd2 = new ProcessBuilder("curl", "-XPUT", indexTypeNum2, "-d",
        "{\"qname\":\"d.e.com\",\"date\":\"2016-02-20T23:29:06.000Z\",\"qtype\":[\"1\"]"
            + ",\"rcode\":\"0\",\"src_ip\":\"127.128.129.130\",\"dest_ip\":\"1.2.3.4\"" + ",\"ip\":[\"5.6.7.8\"]}");

    String indexTypeNum3 = esTestIndex + "/" + esType + "/3";
    logger.info("indexTypeNum3 = " + indexTypeNum3);
    ProcessBuilder pAdd3 = new ProcessBuilder("curl", "-XPUT", indexTypeNum3, "-d",
        "{\"qname\":\"d.e.com\",\"date\":\"2016-02-20T23:29:07.000Z\",\"qtype\":[\"1\"]"
            + ",\"rcode\":\"0\",\"src_ip\":\"131.132.133.134\",\"dest_ip\":\"9.10.11.12\"" + ",\"ip\":[\"13.14.15.16\"]}");

    String indexTypeNum4 = esTestIndex + "/" + esType + "/4";
    logger.info("indexTypeNum4 = " + indexTypeNum4);
    ProcessBuilder pAdd4 = new ProcessBuilder("curl", "-XPUT", indexTypeNum4, "-d",
        "{\"qname\":\"d.e.com\",\"date\":\"2016-02-20T23:29:08.000Z\",\"qtype\":[\"1\"]"
            + ",\"rcode\":\"3\",\"src_ip\":\"135.136.137.138\",\"dest_ip\":\"17.18.19.20\"" + ",\"ip\":[\"21.22.23.24\"]}");

    String indexTypeNum5 = esTestIndex + "/" + esType + "/5";
    logger.info("indexTypeNum5 = " + indexTypeNum5);
    ProcessBuilder pAdd5 = new ProcessBuilder("curl", "-XPUT", indexTypeNum5, "-d",
        "{\"qname\":\"d.e.com\",\"date\":\"2016-02-20T23:29:09.000Z\",\"qtype\":[\"1\"]"
            + ",\"rcode\":\"0\",\"src_ip\":\"139.140.141.142\",\"dest_ip\":\"25.26.27.28\"" + ",\"ip\":[\"5.6.7.8\"]}");

    String indexTypeNum6 = esTestIndex + "/" + esType + "/6";
    logger.info("indexTypeNum6 = " + indexTypeNum6);
    ProcessBuilder pAdd6 = new ProcessBuilder("curl", "-XPUT", indexTypeNum6, "-d",
        "{\"qname\":\"d.e.com\",\"date\":\"2016-02-20T23:29:10.000Z\",\"qtype\":[\"1\"]"
            + ",\"rcode\":\"0\",\"src_ip\":\"143.144.145.146\",\"dest_ip\":\"33.34.35.36\"" + ",\"ip\":[\"5.6.7.8\"]}");

    String indexTypeNum7 = esTestIndex + "/" + esType + "/7";
    logger.info("indexTypeNum7 = " + indexTypeNum7);
    ProcessBuilder pAdd7 = new ProcessBuilder("curl", "-XPUT", indexTypeNum7, "-d",
        "{\"qname\":\"something.else\",\"date\":\"2016-02-20T23:29:11.000Z\",\"qtype\":[\"1\"]"
            + ",\"rcode\":\"0\",\"src_ip\":\"1.1.1.1\",\"dest_ip\":\"2.2.2.2\"" + ",\"ip\":[\"3.3.3.3\"]}");

    // Never should be returned - doesn't hit on any selectors
    String indexTypeNum8 = esTestIndex + "/" + esType + "/8";
    logger.info("indexTypeNum8 = " + indexTypeNum8);
    ProcessBuilder pAdd8 = new ProcessBuilder("curl", "-XPUT", indexTypeNum8, "-d",
        "{\"qname\":\"something.else2\",\"date\":\"2016-02-20T23:29:12.000Z\",\"qtype\":[\"1\"]"
            + ",\"rcode\":\"0\",\"src_ip\":\"1.1.1.12\",\"dest_ip\":\"2.2.2.22\"" + ",\"ip\":[\"3.3.3.32\"]}");

    // This should never be returned -- domain on stoplist
    String indexTypeNum9 = esTestIndex + "/" + esType + "/9";
    logger.info("indexTypeNum9 = " + indexTypeNum9);
    ProcessBuilder pAdd9 = new ProcessBuilder("curl", "-XPUT", indexTypeNum9, "-d",
        "{\"qname\":\"something.else.on.stoplist\",\"date\":\"2016-02-20T23:29:13.000Z\",\"qtype\":[\"1\"]"
            + ",\"rcode\":\"0\",\"src_ip\":\"55.55.55.55\",\"dest_ip\":\"2.2.2.232\"" + ",\"ip\":[\"3.3.3.132\"]}");

    try
    {
      TestUtils.executeCommand(pAdd1);
      TestUtils.executeCommand(pAdd2);
      TestUtils.executeCommand(pAdd3);
      TestUtils.executeCommand(pAdd4);
      TestUtils.executeCommand(pAdd5);
      TestUtils.executeCommand(pAdd6);
      TestUtils.executeCommand(pAdd7);
      TestUtils.executeCommand(pAdd8);
      TestUtils.executeCommand(pAdd9);
    } catch (IOException e)
    {
      e.printStackTrace();
    }

    // Retrieve and print all of the elements
    for (int i = 1; i < 7; ++i)
    {
      logger.info("Retrieving element number = " + i + " from " + esTestIndex);
      String elementGet = esTestIndex + "/" + esType + "/" + i;
      logger.info("elementGet = " + elementGet);
      ProcessBuilder pGet = new ProcessBuilder("curl", "-XGET", elementGet);
      try
      {
        TestUtils.executeCommand(pGet);
      } catch (IOException e)
      {
        e.printStackTrace();
      }
    }
  }

  /**
   * Creates PIR stoplist file
   */
  public static String createPIRStopList(FileSystem fs, boolean hdfs) throws IOException, PIRException
  {
    logger.info("PIR stopList file being created");

    List<String> elements = Arrays.asList("something.else.on.stoplist", "3.3.3.132");

    if (hdfs)
    {
      String pirStopListFile = SystemConfiguration.getProperty(DistributedTestDriver.PIR_STOPLIST_FILE);
      if (pirStopListFile == null)
      {
        throw new PIRException("HDFS stop list file configuration name is required.");
      }
      HDFS.writeFile(elements, fs, pirStopListFile, true);
      logger.info("pirStopListFile file successfully created on hdfs!");
    }

    String prefix = SystemConfiguration.getProperty("pir.stopListFile");
    if (prefix == null)
    {
      throw new PIRException("Local stop list file configuration name is required.");
    }
    return TestUtils.writeToTmpFile(elements, prefix, null);
  }

  /**
   * Create and load the data and query schema files used for testing
   */
  public static void createSchemaFiles(String filter) throws Exception
  {
    createSchemaFiles(null, false, filter);
  }

  /**
   * Create and load the data and query schema files used for testing
   * <p>
   * Writes both local and hdfs schema files if hdfs=true -- only updates the corresponding properties for the local files
   */
  public static void createSchemaFiles(FileSystem fs, boolean hdfs, String filter) throws Exception
  {
    // Create and load the data schema
    if (!hdfs)
    {
      createDataSchema(false);
    }
    else
    {
      createDataSchema(fs, true);
    }
    DataSchemaLoader.initialize();

    // Create and load the query schemas
    // DNS_HOSTNAME_QUERY
    List<String> dnsHostnameQueryElements = Arrays.asList(DATE, SRCIP, DSTIP, QTYPE, RCODE, IPS);
    List<String> dnsHostnameQueryFilterElements = Collections.singletonList(QNAME);

    TestUtils.createQuerySchema(DNS_HOSTNAME_QUERY_FILE, DNS_HOSTNAME_QUERY, TEST_DATA_SCHEMA_NAME, QNAME, dnsHostnameQueryElements,
        dnsHostnameQueryFilterElements, filter);
    if (hdfs)
    {
      TestUtils.createQuerySchema(DNS_HOSTNAME_QUERY_FILE_HDFS, DNS_HOSTNAME_QUERY, TEST_DATA_SCHEMA_NAME, QNAME, dnsHostnameQueryElements,
          dnsHostnameQueryFilterElements, filter, false, fs, hdfs);
    }

    // DNS_IP_QUERY
    List<String> dnsIPQueryElements = Arrays.asList(SRCIP, DSTIP, IPS);
    List<String> dnsIPQueryFilterElements = Collections.singletonList(QNAME);

    TestUtils.createQuerySchema(DNS_IP_QUERY_FILE, DNS_IP_QUERY, TEST_DATA_SCHEMA_NAME, IPS, dnsIPQueryElements, dnsIPQueryFilterElements, filter);
    if (hdfs)
    {
      TestUtils.createQuerySchema(DNS_IP_QUERY_FILE_HDFS, DNS_IP_QUERY, TEST_DATA_SCHEMA_NAME, IPS, dnsIPQueryElements, dnsIPQueryFilterElements, filter,
          false, fs, hdfs);
    }

    // DNS_NXDOMAIN_QUERY
    List<String> dnsNXQueryElements = Arrays.asList(QNAME, SRCIP, DSTIP);
    List<String> dnsNXQueryFilterElements = Collections.singletonList(QNAME);

    TestUtils
        .createQuerySchema(DNS_NXDOMAIN_QUERY_FILE, DNS_NXDOMAIN_QUERY, TEST_DATA_SCHEMA_NAME, QNAME, dnsNXQueryElements, dnsNXQueryFilterElements, filter);
    if (hdfs)
    {
      TestUtils.createQuerySchema(DNS_NXDOMAIN_QUERY_FILE_HDFS, DNS_NXDOMAIN_QUERY, TEST_DATA_SCHEMA_NAME, QNAME, dnsNXQueryElements, dnsNXQueryFilterElements,
          filter, false, fs, hdfs);
    }

    // DNS_SRCIP_QUERY
    List<String> dnsSrcIPQueryElements = Arrays.asList(QNAME, DSTIP, IPS);
    List<String> dnsSrcIPQueryFilterElements = Arrays.asList(SRCIP, IPS);

    TestUtils
        .createQuerySchema(DNS_SRCIP_QUERY_FILE, DNS_SRCIP_QUERY, TEST_DATA_SCHEMA_NAME, SRCIP, dnsSrcIPQueryElements, dnsSrcIPQueryFilterElements, filter);
    if (hdfs)
    {
      TestUtils.createQuerySchema(DNS_SRCIP_QUERY_FILE_HDFS, DNS_SRCIP_QUERY, TEST_DATA_SCHEMA_NAME, SRCIP, dnsSrcIPQueryElements, dnsSrcIPQueryFilterElements,
          filter, false, fs, hdfs);
    }

    // DNS_SRCIP_QUERY_NO_FILTER
    List<String> dnsSrcIPQueryNoFilterElements = Arrays.asList(QNAME, DSTIP, IPS);
    TestUtils.createQuerySchema(DNS_SRCIP_QUERY_NO_FILTER_FILE, DNS_SRCIP_QUERY_NO_FILTER, TEST_DATA_SCHEMA_NAME, SRCIP, dnsSrcIPQueryNoFilterElements, null,
        null);
    if (hdfs)
    {
      TestUtils.createQuerySchema(DNS_SRCIP_QUERY_NO_FILTER_FILE_HDFS, DNS_SRCIP_QUERY_NO_FILTER, TEST_DATA_SCHEMA_NAME, SRCIP, dnsSrcIPQueryNoFilterElements,
          null, null, false, fs, hdfs);
    }

    QuerySchemaLoader.initialize();
  }

  /**
   * Create the test data schema file
   */
  private static void createDataSchema(boolean hdfs) throws IOException
  {
    createDataSchema(null, hdfs);
  }

  /**
   * Create the test data schema file
   */
  private static void createDataSchema(FileSystem fs, boolean hdfs) throws IOException
  {
    // Create a temporary file for the test schema, set in the properties
    File file = File.createTempFile(DATA_SCHEMA_FILE_LOCALFS, ".xml");
    file.deleteOnExit();
    logger.info("file = " + file.toString());
    SystemConfiguration.setProperty("data.schemas", file.toString());

    // If we are performing distributed testing, write both the local and hdfs files
    OutputStreamWriter osw = null;
    if (hdfs)
    {
      Path filePath = new Path(DATA_SCHEMA_FILE_HDFS);
      fs.deleteOnExit(filePath);
      osw = new OutputStreamWriter(fs.create(filePath, true));

      logger.info("hdfs: filePath = " + filePath.toString());
    }

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
      schemaNameElement.appendChild(doc.createTextNode(TEST_DATA_SCHEMA_NAME));
      rootElement.appendChild(schemaNameElement);

      String primitiveTypePartitionerName = PrimitiveTypePartitioner.class.getName();
      String ipPartitionerName = IPDataPartitioner.class.getName();
      String datePartitioner = ISO8601DatePartitioner.class.getName();

      // date
      TestUtils.addElement(doc, rootElement, DATE, PrimitiveTypePartitioner.STRING, "false", datePartitioner);

      // qname
      TestUtils.addElement(doc, rootElement, QNAME, PrimitiveTypePartitioner.STRING, "false", primitiveTypePartitionerName);

      // src_ip
      TestUtils.addElement(doc, rootElement, SRCIP, PrimitiveTypePartitioner.STRING, "false", ipPartitionerName);

      // dest_ip
      TestUtils.addElement(doc, rootElement, DSTIP, PrimitiveTypePartitioner.STRING, "false", ipPartitionerName);

      // qtype
      TestUtils.addElement(doc, rootElement, QTYPE, PrimitiveTypePartitioner.SHORT, "true", primitiveTypePartitionerName);

      // rcode
      TestUtils.addElement(doc, rootElement, RCODE, PrimitiveTypePartitioner.INT, "false", primitiveTypePartitionerName);

      // ip
      TestUtils.addElement(doc, rootElement, IPS, PrimitiveTypePartitioner.STRING, "true", ipPartitionerName);

      // Write to a xml file - both localFS and hdfs
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      DOMSource source = new DOMSource(doc);

      // LocalFS
      StreamResult resultLocalFS = new StreamResult(file);
      transformer.transform(source, resultLocalFS);

      if (hdfs)
      {
        StreamResult resultHDFS = new StreamResult(osw);
        transformer.transform(source, resultHDFS);
      }

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
}
