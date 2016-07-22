/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.apache.pirk.query.wideskies;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.apache.pirk.utils.LogUtils;

/**
 * Class to hold all of the basic information regarding a query
 * <p>
 * Note that the hash key is specific to the query. If we have hash collisions over our selector set, we will append integers to the key starting with 0 until
 * we no longer have collisions
 *
 */
public class QueryInfo implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static Logger logger = LogUtils.getLoggerForThisClass();

  double queryNum = 0.0; // the identifier num of the query
  int numSelectors = 0; // the number of selectors in the query, given by \floor{paillerBitSize/dataPartitionBitSize}

  String queryType = null; // QueryType string const

  String queryName = null; // Name of query

  int paillierBitSize = 0; // Paillier modulus size

  int hashBitSize = 0; // Bit size of the keyed hash function
  String hashKey = null; // Key for the keyed hash function

  int numBitsPerDataElement = 0; // total num bits per returned data value - defined relative to query type
  int dataPartitionBitSize = 0; // num of bits for each partition of an incoming data element, must be < 32 right now
  int numPartitionsPerDataElement = 0; // num partitions of size dataPartitionBitSize per data element

  boolean useExpLookupTable = false; // whether or not to generate and use the expLookupTable for encryption, it is very expensive to compute

  boolean useHDFSExpLookupTable = false; // whether or not to use the expLookupTable stored in HDFS
  // if it doesn't yet exist, it will be created within the cluster and stored in HDFS

  boolean embedSelector = true; // whether or not to embed the selector in the results - results in a very low

  // false positive rate for variable length selectors and a zero false positive rate
  // for selectors of fixed size < 32 bits

  public QueryInfo(double queryNumInput, int numSelectorsInput, int hashBitSizeInput, String hashKeyInput, int dataPartitionBitSizeInput,
      String queryTypeInput, String queryNameInput, int paillierBitSizeIn, boolean useExpLookupTableInput, boolean embedSelectorInput,
      boolean useHDFSExpLookupTableInput)
  {
    queryNum = queryNumInput;
    queryType = queryTypeInput;
    queryName = queryNameInput;

    numSelectors = numSelectorsInput;

    hashBitSize = hashBitSizeInput;
    hashKey = hashKeyInput;

    useExpLookupTable = useExpLookupTableInput;
    useHDFSExpLookupTable = useHDFSExpLookupTableInput;
    embedSelector = embedSelectorInput;

    numBitsPerDataElement = LoadQuerySchemas.getSchema(queryType).getDataElementSize();
    dataPartitionBitSize = dataPartitionBitSizeInput;
    numPartitionsPerDataElement = numBitsPerDataElement / dataPartitionBitSizeInput;

    paillierBitSize = paillierBitSizeIn;

    if (embedSelectorInput)
    {
      numPartitionsPerDataElement += 4; // using a 8-bit partition size and a 32-bit embedded selector
    }

    printQueryInfo();
  }

  public QueryInfo(double queryNumInput, int numSelectorsInput, int hashBitSizeInput, String hashKeyInput, int dataPartitionBitSizeInput,
      String queryTypeInput, String queryNameInput, int paillierBitSizeIn)
  {
    this(queryNumInput, numSelectorsInput, hashBitSizeInput, hashKeyInput, dataPartitionBitSizeInput, queryTypeInput, queryNameInput, paillierBitSizeIn, false,
        true, false);
  }

  public QueryInfo(Map queryInfoMap)
  {
    queryNum = (double) queryInfoMap.get("queryNum");
    queryType = (String) queryInfoMap.get("queryType");
    queryName = (String) queryInfoMap.get("queryName");
    numSelectors = ((Long) queryInfoMap.get("numSelectors")).intValue();
    paillierBitSize = ((Long) queryInfoMap.get("paillierBitSize")).intValue();
    hashBitSize = ((Long) queryInfoMap.get("hashBitSize")).intValue();
    hashKey = (String) queryInfoMap.get("hashKey");
    numBitsPerDataElement = ((Long) queryInfoMap.get("numBitsPerDataElement")).intValue();
    numPartitionsPerDataElement = ((Long) queryInfoMap.get("numPartitionsPerDataElement")).intValue();
    dataPartitionBitSize = ((Long) queryInfoMap.get("dataPartitionsBitSize")).intValue();
    useExpLookupTable = (boolean) queryInfoMap.get("useExpLookupTable");
    useHDFSExpLookupTable = (boolean) queryInfoMap.get("useHDFSExpLookupTable");
    embedSelector = (boolean) queryInfoMap.get("embedSelector");
  }

  public double getQueryNum()
  {
    return queryNum;
  }

  public String getQueryType()
  {
    return queryType;
  }

  public String getQueryName()
  {
    return queryName;
  }

  public int getNumSelectors()
  {
    return numSelectors;
  }

  public int getPaillierBitSize()
  {
    return paillierBitSize;
  }

  public int getHashBitSize()
  {
    return hashBitSize;
  }

  public String getHashKey()
  {
    return hashKey;
  }

  public int getNumBitsPerDataElement()
  {
    return numBitsPerDataElement;
  }

  public int getNumPartitionsPerDataElement()
  {
    return numPartitionsPerDataElement;
  }

  public int getDataPartitionBitSize()
  {
    return dataPartitionBitSize;
  }

  public boolean getUseExpLookupTable()
  {
    return useExpLookupTable;
  }

  public boolean getUseHDFSExpLookupTable()
  {
    return useHDFSExpLookupTable;
  }

  public boolean getEmbedSelector()
  {
    return embedSelector;
  }

  public Map toMap()
  {
    HashMap<String,Object> queryInfo = new HashMap<String,Object>();
    queryInfo.put("queryNum", queryNum);
    queryInfo.put("queryType", queryType);
    queryInfo.put("queryName", queryName);
    queryInfo.put("numSelectors", numSelectors);
    queryInfo.put("paillierBitSize", paillierBitSize);
    queryInfo.put("hashBitSize", hashBitSize);
    queryInfo.put("hashKey", hashKey);
    queryInfo.put("numBitsPerDataElement", numBitsPerDataElement);
    queryInfo.put("numPartitionsPerDataElement", numPartitionsPerDataElement);
    queryInfo.put("dataPartitionsBitSize", dataPartitionBitSize);
    queryInfo.put("useExpLookupTable", useExpLookupTable);
    queryInfo.put("useHDFSExpLookupTable", useHDFSExpLookupTable);
    queryInfo.put("embedSelector", embedSelector);

    return queryInfo;
  }

  public void printQueryInfo()
  {
    logger.info("queryNum = " + queryNum + " numSelectors = " + numSelectors + " hashBitSize = " + hashBitSize + " hashKey = " + hashKey
        + " dataPartitionBitSize = " + dataPartitionBitSize + " numBitsPerDataElement = " + numBitsPerDataElement + " numPartitionsPerDataElement = "
        + numPartitionsPerDataElement + " queryType = " + queryType + " queryName = " + queryName + " paillierBitSize = " + paillierBitSize
        + " useExpLookupTable = " + useExpLookupTable + " useHDFSExpLookupTable = " + useHDFSExpLookupTable + " embedSelector = " + embedSelector);
  }

  public QueryInfo copy()
  {
    QueryInfo queryInfo = new QueryInfo(this.queryNum, this.numSelectors, this.hashBitSize, this.hashKey, this.dataPartitionBitSize, this.queryType,
        this.queryName, this.paillierBitSize, this.useExpLookupTable, this.embedSelector, this.useHDFSExpLookupTable);

    return queryInfo;
  }
}
