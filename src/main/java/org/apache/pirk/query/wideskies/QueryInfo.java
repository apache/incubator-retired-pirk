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
package org.apache.pirk.query.wideskies;

import java.io.Serializable;
import java.util.UUID;

import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold all of the basic information regarding a query
 * <p>
 * Note that the hash key is specific to the query. If we have hash collisions over our selector set, we will append integers to the key starting with 0 until
 * we no longer have collisions
 * 
 */
public class QueryInfo implements Serializable, Cloneable
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(QueryInfo.class);

  private UUID identifier; // the identifier of the query
  private int numSelectors = 0; // the number of selectors in the query, given by \floor{paillerBitSize/dataPartitionBitSize}

  private String queryType = null; // QueryType string const

  private int hashBitSize = 0; // Bit size of the keyed hash function
  private String hashKey = null; // Key for the keyed hash function

  private int numBitsPerDataElement = 0; // total num bits per returned data value - defined relative to query type
  private int dataPartitionBitSize = 0; // num of bits for each partition of an incoming data element, must be < 32 right now
  private int numPartitionsPerDataElement = 0; // num partitions of size dataPartitionBitSize per data element

  private boolean useExpLookupTable = false; // whether or not to generate and use the expLookupTable for encryption, it is very expensive to compute

  private boolean useHDFSExpLookupTable = false; // whether or not to use the expLookupTable stored in HDFS
  // if it doesn't yet exist, it will be created within the cluster and stored in HDFS

  private boolean embedSelector = true; // whether or not to embed the selector in the results - results in a very low

  // false positive rate for variable length selectors and a zero false positive rate
  // for selectors of fixed size < 32 bits

  QuerySchema qSchema = null;

  public QueryInfo(int numSelectorsInput, int hashBitSizeInput, String hashKeyInput, int dataPartitionBitSizeInput, String queryTypeInput,
       boolean useExpLookupTableInput, boolean embedSelectorInput,
       boolean useHDFSExpLookupTableInput)
  {
    this(UUID.randomUUID(), numSelectorsInput, hashBitSizeInput, hashKeyInput, dataPartitionBitSizeInput, queryTypeInput,
        useExpLookupTableInput, embedSelectorInput, useHDFSExpLookupTableInput);
  }

  public QueryInfo(
      UUID identifierInput,
      int numSelectorsInput,
      int hashBitSizeInput,
      String hashKeyInput,
      int dataPartitionBitSizeInput,
      String queryTypeInput,
      boolean useExpLookupTableInput,
      boolean embedSelectorInput,
      boolean useHDFSExpLookupTableInput)
  {
    identifier = identifierInput;
    queryType = queryTypeInput;

    numSelectors = numSelectorsInput;

    hashBitSize = hashBitSizeInput;
    hashKey = hashKeyInput;

    useExpLookupTable = useExpLookupTableInput;
    useHDFSExpLookupTable = useHDFSExpLookupTableInput;
    embedSelector = embedSelectorInput;

    numBitsPerDataElement = QuerySchemaRegistry.get(queryType).getDataElementSize();
    dataPartitionBitSize = dataPartitionBitSizeInput;
    numPartitionsPerDataElement = numBitsPerDataElement / dataPartitionBitSizeInput;

    if (embedSelectorInput)
    {
      numPartitionsPerDataElement += 4; // using a 8-bit partition size and a 32-bit embedded selector
    }

    printQueryInfo();
  }
  
  public UUID getIdentifier()
  {
    return identifier;
  }

  public String getQueryType()
  {
    return queryType;
  }

  public int getNumSelectors()
  {
    return numSelectors;
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

  public void addQuerySchema(QuerySchema qSchemaIn)
  {
    qSchema = qSchemaIn;
  }

  public QuerySchema getQuerySchema()
  {
    return qSchema;
  }

  public void printQueryInfo()
  {
    logger.info("identifier = " + identifier + " numSelectors = " + numSelectors + " hashBitSize = " + hashBitSize + " hashKey = " + hashKey
        + " dataPartitionBitSize = " + dataPartitionBitSize + " numBitsPerDataElement = " + numBitsPerDataElement + " numPartitionsPerDataElement = "
        + numPartitionsPerDataElement + " queryType = " + queryType
        + " useExpLookupTable = " + useExpLookupTable + " useHDFSExpLookupTable = " + useHDFSExpLookupTable + " embedSelector = " + embedSelector);
  }

  @Override
  public QueryInfo clone()
  {
    try
    {
      return (QueryInfo) super.clone();
    } catch (CloneNotSupportedException e)
    {
      throw new RuntimeException(e);
    }
  }
}
