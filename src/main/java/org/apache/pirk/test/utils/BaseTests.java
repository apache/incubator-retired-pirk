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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.pirk.query.wideskies.QueryUtils;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaRegistry;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.test.distributed.testsuite.DistTestSuite;
import org.apache.pirk.utils.StringUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold the base functional distributed tests
 */
public class BaseTests
{
  private static final Logger logger = LoggerFactory.getLogger(BaseTests.class);

  public static double queryNum = 1.0;
  public static int dataPartitionBitSize = 8;

  // Selectors for domain and IP queries, queryNum is the first entry for file generation
  private static ArrayList<String> selectorsDomain = new ArrayList<>(Arrays.asList("s.t.u.net", "d.e.com", "r.r.r.r", "a.b.c.com", "something.else", "x.y.net"));
  private static ArrayList<String> selectorsIP = new ArrayList<>(Arrays.asList("55.55.55.55", "5.6.7.8", "10.20.30.40", "13.14.15.16", "21.22.23.24"));

  // Encryption variables -- Paillier mechanisms are tested in the Paillier test code, so these are fixed...
  public static int hashBitSize = 12;
  public static String hashKey = "someKey";
  public static int paillierBitSize = 384;
  public static int certainty = 128;

  public static void testDNSHostnameQuery(ArrayList<JSONObject> dataElements, int numThreads, boolean testFalsePositive) throws Exception
  {
    testDNSHostnameQuery(dataElements, null, false, false, numThreads, testFalsePositive);
  }

  public static void testDNSHostnameQuery(ArrayList<JSONObject> dataElements, FileSystem fs, boolean isSpark, boolean isDistributed, int numThreads)
      throws Exception
  {
    testDNSHostnameQuery(dataElements, fs, isSpark, isDistributed, numThreads, false);
  }

  // Query for the watched hostname occurred; ; watched value type: hostname (String)
  public static void testDNSHostnameQuery(ArrayList<JSONObject> dataElements, FileSystem fs, boolean isSpark, boolean isDistributed, int numThreads,
      boolean testFalsePositive) throws Exception
  {
    logger.info("Running testDNSHostnameQuery(): ");

    int numExpectedResults = 6;
    ArrayList<QueryResponseJSON> results;
    if (isDistributed)
    {
      results = DistTestSuite.performQuery(Inputs.DNS_HOSTNAME_QUERY, selectorsDomain, fs, isSpark, numThreads);
    }
    else
    {
      results = StandaloneQuery.performStandaloneQuery(dataElements, Inputs.DNS_HOSTNAME_QUERY, selectorsDomain, numThreads, testFalsePositive);
      if (!testFalsePositive)
      {
        numExpectedResults = 7; // all 7 for non distributed case; if testFalsePositive==true, then 6
      }
    }
    checkDNSHostnameQueryResults(results, isDistributed, numExpectedResults, testFalsePositive, dataElements);
    logger.info("Completed testDNSHostnameQuery(): ");
  }

  public static void checkDNSHostnameQueryResults(ArrayList<QueryResponseJSON> results, boolean isDistributed, int numExpectedResults,
      boolean testFalsePositive, ArrayList<JSONObject> dataElements)
  {
    QuerySchema qSchema = QuerySchemaRegistry.get(Inputs.DNS_HOSTNAME_QUERY);
    logger.info("results:");
    printResultList(results);

    if (isDistributed && SystemConfiguration.getProperty("pir.limitHitsPerSelector").equals("true"))
    {
      // 3 elements returned - one for each qname -- a.b.c.com, d.e.com, something.else
      if (results.size() != 3)
      {
        fail("results.size() = " + results.size() + " -- must equal 3");
      }

      // Check that each qname appears once in the result set
      HashSet<String> correctQnames = new HashSet<>();
      correctQnames.add("a.b.c.com");
      correctQnames.add("d.e.com");
      correctQnames.add("something.else");

      HashSet<String> resultQnames = new HashSet<>();
      for (QueryResponseJSON qrJSON : results)
      {
        resultQnames.add((String) qrJSON.getValue(Inputs.QNAME));
      }

      if (correctQnames.size() != resultQnames.size())
      {
        fail("correctQnames.size() = " + correctQnames.size() + " != resultQnames.size() " + resultQnames.size());
      }

      for (String resultQname : resultQnames)
      {
        if (!correctQnames.contains(resultQname))
        {
          fail("correctQnames does not contain resultQname = " + resultQname);
        }
      }
    }
    else
    {
      if (results.size() != numExpectedResults)
      {
        fail("results.size() = " + results.size() + " -- must equal " + numExpectedResults);
      }

      // Number of original elements at the end of the list that we do not need to consider for hits
      int removeTailElements = 2; // the last two data elements should not hit
      if (testFalsePositive)
      {
        removeTailElements = 3;
      }

      ArrayList<QueryResponseJSON> correctResults = new ArrayList<>();
      int i = 0;
      while (i < (dataElements.size() - removeTailElements))
      {
        JSONObject dataMap = dataElements.get(i);

        boolean addElement = true;
        if (isDistributed && dataMap.get(Inputs.RCODE).toString().equals("3"))
        {
          addElement = false;
        }
        if (addElement)
        {
          QueryResponseJSON wlJSON = new QueryResponseJSON();
          wlJSON.setMapping(QueryResponseJSON.QUERY_ID, queryNum);
          wlJSON.setMapping(QueryResponseJSON.QUERY_NAME, Inputs.DNS_HOSTNAME_QUERY + "_" + queryNum);
          wlJSON.setMapping(QueryResponseJSON.EVENT_TYPE, Inputs.DNS_HOSTNAME_QUERY);
          wlJSON.setMapping(Inputs.DATE, dataMap.get(Inputs.DATE));
          wlJSON.setMapping(Inputs.SRCIP, dataMap.get(Inputs.SRCIP));
          wlJSON.setMapping(Inputs.DSTIP, dataMap.get(Inputs.DSTIP));
          wlJSON.setMapping(Inputs.QNAME, dataMap.get(Inputs.QNAME)); // this gets re-embedded as the original selector after decryption
          wlJSON.setMapping(Inputs.QTYPE, parseShortArray(dataMap, Inputs.QTYPE));
          wlJSON.setMapping(Inputs.RCODE, dataMap.get(Inputs.RCODE));
          wlJSON.setMapping(Inputs.IPS, parseArray(dataMap, Inputs.IPS, true));
          wlJSON.setMapping(QueryResponseJSON.SELECTOR, QueryUtils.getSelectorByQueryTypeJSON(qSchema, dataMap));
          correctResults.add(wlJSON);
        }
        ++i;
      }
      logger.info("correctResults: ");
      printResultList(correctResults);

      if (results.size() != correctResults.size())
      {
        logger.info("correctResults:");
        printResultList(correctResults);
        fail("results.size() = " + results.size() + " != correctResults.size() = " + correctResults.size());
      }
      for (QueryResponseJSON result : results)
      {
        if (!compareResultArray(correctResults, result))
        {
          fail("correctResults does not contain result = " + result.toString());
        }
      }
    }
  }

  public static void testDNSIPQuery(ArrayList<JSONObject> dataElements, int numThreads) throws Exception
  {
    testDNSIPQuery(dataElements, null, false, false, numThreads);
  }

  // The watched IP address was detected in the response to a query; watched value type: IP address (String)
  public static void testDNSIPQuery(ArrayList<JSONObject> dataElements, FileSystem fs, boolean isSpark, boolean isDistributed, int numThreads) throws Exception
  {
    logger.info("Running testDNSIPQuery(): ");

    QuerySchema qSchema = QuerySchemaRegistry.get(Inputs.DNS_IP_QUERY);
    ArrayList<QueryResponseJSON> results;

    if (isDistributed)
    {
      results = DistTestSuite.performQuery(Inputs.DNS_IP_QUERY, selectorsIP, fs, isSpark, numThreads);

      if (results.size() != 5)
      {
        fail("results.size() = " + results.size() + " -- must equal 5");
      }
    }
    else
    {
      results = StandaloneQuery.performStandaloneQuery(dataElements, Inputs.DNS_IP_QUERY, selectorsIP, numThreads, false);

      if (results.size() != 6)
      {
        fail("results.size() = " + results.size() + " -- must equal 6");
      }
    }
    printResultList(results);

    ArrayList<QueryResponseJSON> correctResults = new ArrayList<>();
    int i = 0;
    while (i < (dataElements.size() - 3)) // last three data elements not hit - one on stoplist, two don't match selectors
    {
      JSONObject dataMap = dataElements.get(i);

      boolean addElement = true;
      if (isDistributed && dataMap.get(Inputs.RCODE).toString().equals("3"))
      {
        addElement = false;
      }
      if (addElement)
      {
        QueryResponseJSON wlJSON = new QueryResponseJSON();
        wlJSON.setMapping(QueryResponseJSON.QUERY_ID, queryNum);
        wlJSON.setMapping(QueryResponseJSON.QUERY_NAME, Inputs.DNS_IP_QUERY + "_" + queryNum);
        wlJSON.setMapping(QueryResponseJSON.EVENT_TYPE, Inputs.DNS_IP_QUERY);
        wlJSON.setMapping(Inputs.SRCIP, dataMap.get(Inputs.SRCIP));
        wlJSON.setMapping(Inputs.DSTIP, dataMap.get(Inputs.DSTIP));
        wlJSON.setMapping(Inputs.IPS, parseArray(dataMap, Inputs.IPS, true));
        wlJSON.setMapping(QueryResponseJSON.SELECTOR, QueryUtils.getSelectorByQueryTypeJSON(qSchema, dataMap));
        correctResults.add(wlJSON);
      }
      ++i;
    }
    if (results.size() != correctResults.size())
    {
      logger.info("correctResults:");
      printResultList(correctResults);
      fail("results.size() = " + results.size() + " != correctResults.size() = " + correctResults.size());
    }
    for (QueryResponseJSON result : results)
    {
      if (!compareResultArray(correctResults, result))
      {
        fail("correctResults does not contain result = " + result.toString());
      }
    }
    logger.info("Completed testDNSIPQuery(): ");
  }

  public static void testDNSNXDOMAINQuery(ArrayList<JSONObject> dataElements, int numThreads) throws Exception
  {
    testDNSNXDOMAINQuery(dataElements, null, false, false, numThreads);
  }

  // A query that returned an nxdomain response was made for the watched hostname; watched value type: hostname (String)
  public static void testDNSNXDOMAINQuery(ArrayList<JSONObject> dataElements, FileSystem fs, boolean isSpark, boolean isDistributed, int numThreads)
      throws Exception
  {
    logger.info("Running testDNSNXDOMAINQuery(): ");

    QuerySchema qSchema = QuerySchemaRegistry.get(Inputs.DNS_NXDOMAIN_QUERY);
    ArrayList<QueryResponseJSON> results;

    if (isDistributed)
    {
      results = DistTestSuite.performQuery(Inputs.DNS_NXDOMAIN_QUERY, selectorsDomain, fs, isSpark, numThreads);
    }
    else
    {
      results = StandaloneQuery.performStandaloneQuery(dataElements, Inputs.DNS_NXDOMAIN_QUERY, selectorsDomain, numThreads, false);
    }
    printResultList(results);

    if (results.size() != 1)
    {
      fail("results.size() = " + results.size() + " -- must equal 1");
    }

    ArrayList<QueryResponseJSON> correctResults = new ArrayList<>();
    int i = 0;
    while (i < dataElements.size())
    {
      JSONObject dataMap = dataElements.get(i);

      if (dataMap.get(Inputs.RCODE).toString().equals("3"))
      {
        QueryResponseJSON wlJSON = new QueryResponseJSON();
        wlJSON.setMapping(QueryResponseJSON.QUERY_ID, queryNum);
        wlJSON.setMapping(QueryResponseJSON.QUERY_NAME, Inputs.DNS_NXDOMAIN_QUERY + "_" + queryNum);
        wlJSON.setMapping(QueryResponseJSON.EVENT_TYPE, Inputs.DNS_NXDOMAIN_QUERY);
        wlJSON.setMapping(Inputs.QNAME, dataMap.get(Inputs.QNAME)); // this gets re-embedded as the original selector after decryption
        wlJSON.setMapping(Inputs.DSTIP, dataMap.get(Inputs.DSTIP));
        wlJSON.setMapping(Inputs.SRCIP, dataMap.get(Inputs.SRCIP));
        wlJSON.setMapping(QueryResponseJSON.SELECTOR, QueryUtils.getSelectorByQueryTypeJSON(qSchema, dataMap));
        correctResults.add(wlJSON);
      }
      ++i;
    }
    if (results.size() != correctResults.size())
    {
      logger.info("correctResults:");
      printResultList(correctResults);
      fail("results.size() = " + results.size() + " != correctResults.size() = " + correctResults.size());
    }
    for (QueryResponseJSON result : results)
    {
      if (!compareResultArray(correctResults, result))
      {
        fail("correctResults does not contain result = " + result.toString());
      }
    }
    logger.info("Completed testDNSNXDOMAINQuery(): ");
  }

  public static void testSRCIPQuery(ArrayList<JSONObject> dataElements, int numThreads) throws Exception
  {
    testSRCIPQuery(dataElements, null, false, false, numThreads);
  }

  // Query for responses from watched srcIPs
  public static void testSRCIPQuery(ArrayList<JSONObject> dataElements, FileSystem fs, boolean isSpark, boolean isDistributed, int numThreads) throws Exception
  {
    logger.info("Running testSRCIPQuery(): ");

    QuerySchema qSchema = QuerySchemaRegistry.get(Inputs.DNS_SRCIP_QUERY);
    ArrayList<QueryResponseJSON> results;

    int removeTailElements = 0;
    int numExpectedResults = 1;
    if (isDistributed)
    {
      results = DistTestSuite.performQuery(Inputs.DNS_SRCIP_QUERY, selectorsIP, fs, isSpark, numThreads);
      removeTailElements = 2; // The last two elements are on the distributed stoplist
    }
    else
    {
      numExpectedResults = 3;
      results = StandaloneQuery.performStandaloneQuery(dataElements, Inputs.DNS_SRCIP_QUERY, selectorsIP, numThreads, false);
    }
    printResultList(results);

    if (results.size() != numExpectedResults)
    {
      fail("results.size() = " + results.size() + " -- must equal " + numExpectedResults);
    }

    ArrayList<QueryResponseJSON> correctResults = new ArrayList<>();
    int i = 0;
    while (i < (dataElements.size() - removeTailElements))
    {
      JSONObject dataMap = dataElements.get(i);

      boolean addElement = false;
      if (dataMap.get(Inputs.SRCIP).toString().equals("55.55.55.55") || dataMap.get(Inputs.SRCIP).toString().equals("5.6.7.8"))
      {
        addElement = true;
      }
      if (addElement)
      {
        // Form the correct result QueryResponseJSON object
        QueryResponseJSON qrJSON = new QueryResponseJSON();
        qrJSON.setMapping(QueryResponseJSON.QUERY_ID, queryNum);
        qrJSON.setMapping(QueryResponseJSON.QUERY_NAME, Inputs.DNS_SRCIP_QUERY + "_" + queryNum);
        qrJSON.setMapping(QueryResponseJSON.EVENT_TYPE, Inputs.DNS_SRCIP_QUERY);
        qrJSON.setMapping(Inputs.QNAME, parseString(dataMap, Inputs.QNAME));
        qrJSON.setMapping(Inputs.DSTIP, dataMap.get(Inputs.DSTIP));
        qrJSON.setMapping(Inputs.SRCIP, dataMap.get(Inputs.SRCIP));
        qrJSON.setMapping(Inputs.IPS, parseArray(dataMap, Inputs.IPS, true));
        qrJSON.setMapping(QueryResponseJSON.SELECTOR, QueryUtils.getSelectorByQueryTypeJSON(qSchema, dataMap));
        correctResults.add(qrJSON);
      }
      ++i;
    }
    logger.info("correctResults:");
    printResultList(correctResults);

    if (results.size() != correctResults.size())
    {
      logger.info("correctResults:");
      printResultList(correctResults);
      fail("results.size() = " + results.size() + " != correctResults.size() = " + correctResults.size());
    }
    for (QueryResponseJSON result : results)
    {
      if (!compareResultArray(correctResults, result))
      {
        fail("correctResults does not contain result = " + result.toString());
      }
    }
    logger.info("Completed testSRCIPQuery(): ");
  }

  // Query for responses from watched srcIPs
  public static void testSRCIPQueryNoFilter(ArrayList<JSONObject> dataElements, FileSystem fs, boolean isSpark, boolean isDistributed, int numThreads)
      throws Exception
  {
    logger.info("Running testSRCIPQueryNoFilter(): ");

    QuerySchema qSchema = QuerySchemaRegistry.get(Inputs.DNS_SRCIP_QUERY_NO_FILTER);
    ArrayList<QueryResponseJSON> results;

    int numExpectedResults = 3;
    if (isDistributed)
    {
      results = DistTestSuite.performQuery(Inputs.DNS_SRCIP_QUERY_NO_FILTER, selectorsIP, fs, isSpark, numThreads);
    }
    else
    {
      results = StandaloneQuery.performStandaloneQuery(dataElements, Inputs.DNS_SRCIP_QUERY_NO_FILTER, selectorsIP, numThreads, false);
    }
    printResultList(results);

    if (results.size() != numExpectedResults)
    {
      fail("results.size() = " + results.size() + " -- must equal " + numExpectedResults);
    }

    ArrayList<QueryResponseJSON> correctResults = new ArrayList<>();
    int i = 0;
    while (i < dataElements.size())
    {
      JSONObject dataMap = dataElements.get(i);

      boolean addElement = false;
      if (dataMap.get(Inputs.SRCIP).toString().equals("55.55.55.55") || dataMap.get(Inputs.SRCIP).toString().equals("5.6.7.8"))
      {
        addElement = true;
      }
      if (addElement)
      {
        // Form the correct result QueryResponseJSON object
        QueryResponseJSON qrJSON = new QueryResponseJSON();
        qrJSON.setMapping(QueryResponseJSON.QUERY_ID, queryNum);
        qrJSON.setMapping(QueryResponseJSON.QUERY_NAME, Inputs.DNS_SRCIP_QUERY_NO_FILTER + "_" + queryNum);
        qrJSON.setMapping(QueryResponseJSON.EVENT_TYPE, Inputs.DNS_SRCIP_QUERY_NO_FILTER);
        qrJSON.setMapping(Inputs.QNAME, parseString(dataMap, Inputs.QNAME));
        qrJSON.setMapping(Inputs.DSTIP, dataMap.get(Inputs.DSTIP));
        qrJSON.setMapping(Inputs.SRCIP, dataMap.get(Inputs.SRCIP));
        qrJSON.setMapping(Inputs.IPS, parseArray(dataMap, Inputs.IPS, true));
        qrJSON.setMapping(QueryResponseJSON.SELECTOR, QueryUtils.getSelectorByQueryTypeJSON(qSchema, dataMap));
        correctResults.add(qrJSON);
      }
      ++i;
    }
    logger.info("correctResults:");
    printResultList(correctResults);

    if (results.size() != correctResults.size())
    {
      logger.info("correctResults:");
      printResultList(correctResults);
      fail("results.size() = " + results.size() + " != correctResults.size() = " + correctResults.size());
    }
    for (QueryResponseJSON result : results)
    {
      if (!compareResultArray(correctResults, result))
      {
        fail("correctResults does not contain result = " + result.toString());
      }
    }
    logger.info("Completed testSRCIPQueryNoFilter(): ");
  }

  @SuppressWarnings("unchecked")
  // Method to convert a ArrayList<String> into the correct (padded) returned ArrayList
  private static ArrayList<String> parseArray(JSONObject dataMap, String fieldName, boolean isIP)
  {
    ArrayList<String> retArray = new ArrayList<>();

    ArrayList<String> values;
    if (dataMap.get(fieldName) instanceof ArrayList)
    {
      values = (ArrayList<String>) dataMap.get(fieldName);
    }
    else
    {
      values = StringUtils.jsonArrayStringToArrayList((String) dataMap.get(fieldName));
    }

    int numArrayElementsToReturn = Integer.parseInt(SystemConfiguration.getProperty("pir.numReturnArrayElements", "1"));
    for (int i = 0; i < numArrayElementsToReturn; ++i)
    {
      if (i < values.size())
      {
        retArray.add(values.get(i));
      }
      else if (isIP)
      {
        retArray.add("0.0.0.0");
      }
      else
      {
        retArray.add("0");
      }
    }

    return retArray;
  }

  // Method to convert a ArrayList<Short> into the correct (padded) returned ArrayList
  private static ArrayList<Short> parseShortArray(JSONObject dataMap, String fieldName)
  {
    ArrayList<Short> retArray = new ArrayList<>();

    ArrayList<Short> values = (ArrayList<Short>) dataMap.get(fieldName);

    int numArrayElementsToReturn = Integer.parseInt(SystemConfiguration.getProperty("pir.numReturnArrayElements", "1"));
    for (int i = 0; i < numArrayElementsToReturn; ++i)
    {
      if (i < values.size())
      {
        retArray.add(values.get(i));
      }
      else
      {
        retArray.add((short) 0);
      }
    }

    return retArray;
  }

  // Method to convert the String field value to the correct returned substring
  private static String parseString(JSONObject dataMap, String fieldName)
  {
    String ret;

    String element = (String) dataMap.get(fieldName);
    int numParts = Integer.parseInt(SystemConfiguration.getProperty("pir.stringBits")) / dataPartitionBitSize;
    int len = numParts;
    if (element.length() < numParts)
    {
      len = element.length();
    }
    ret = new String(element.getBytes(), 0, len);

    return ret;
  }

  // Method to determine whether or not the correctResults contains an object equivalent to
  // the given result
  private static boolean compareResultArray(ArrayList<QueryResponseJSON> correctResults, QueryResponseJSON result)
  {
    boolean equivalent = false;

    for (QueryResponseJSON correct : correctResults)
    {
      equivalent = compareResults(correct, result);
      if (equivalent)
      {
        break;
      }
    }

    return equivalent;
  }

  // Method to test the equivalence of two test results
  private static boolean compareResults(QueryResponseJSON r1, QueryResponseJSON r2)
  {
    boolean equivalent = true;

    JSONObject jsonR1 = r1.getJSONObject();
    JSONObject jsonR2 = r2.getJSONObject();

    Set<String> r1KeySet = jsonR1.keySet();
    Set<String> r2KeySet = jsonR2.keySet();
    if (!r1KeySet.equals(r2KeySet))
    {
      equivalent = false;
    }
    if (equivalent)
    {
      for (String key : r1KeySet)
      {
        if (key.equals(Inputs.QTYPE) || key.equals(Inputs.IPS)) // array types
        {
          HashSet<String> set1 = getSetFromList(jsonR1.get(key));
          HashSet<String> set2 = getSetFromList(jsonR2.get(key));

          if (!set1.equals(set2))
          {
            equivalent = false;
          }
        }
        else
        {
          if (!(jsonR1.get(key).toString()).equals(jsonR2.get(key).toString()))
          {
            equivalent = false;
          }
        }
      }
    }
    return equivalent;
  }

  // Method to pull the elements of a list (either an ArrayList or JSONArray) into a HashSet
  private static HashSet<String> getSetFromList(Object list)
  {
    HashSet<String> set = new HashSet<>();

    if (list instanceof ArrayList)
    {
      for (Object obj : (ArrayList) list)
      {
        set.add(obj.toString());
      }
    }
    else
    // JSONArray
    {
      for (Object obj : (JSONArray) list)
      {
        set.add(obj.toString());
      }
    }

    return set;
  }

  private static void printResultList(ArrayList<QueryResponseJSON> list)
  {
    for (QueryResponseJSON obj : list)
    {
      logger.info(obj.toString());
    }
  }
}
