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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.data.partitioner.DataPartitioner;
import org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.utils.KeyedHash;
import org.apache.pirk.utils.StringUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for helper methods to perform the encrypted query
 */
public class QueryUtils
{
  private static final Logger logger = LoggerFactory.getLogger(QueryUtils.class);

  /**
   * Method to convert the given BigInteger raw data element partitions to a QueryResponseJSON object based upon the given queryType
   */
  public static QueryResponseJSON extractQueryResponseJSON(QueryInfo queryInfo, QuerySchema qSchema, ArrayList<BigInteger> parts) throws Exception
  {
    QueryResponseJSON qrJSON = new QueryResponseJSON(queryInfo);

    DataSchema dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());

    int numArrayElementsToReturn = Integer.parseInt(SystemConfiguration.getProperty("pir.numReturnArrayElements", "1"));

    logger.debug("parts.size() = " + parts.size());

    int partsIndex = 0;
    if (queryInfo.getEmbedSelector())
    {
      String selectorFieldName = qSchema.getSelectorName();
      String type = dSchema.getElementType(selectorFieldName);
      String embeddedSelector = getEmbeddedSelectorFromPartitions(parts, partsIndex, type, (dSchema.getPartitionerForElement(selectorFieldName)));

      qrJSON.setSelector(embeddedSelector);
      partsIndex += 4;

      logger.debug("Extracted embedded selector = " + embeddedSelector + " parts.size() = " + parts.size());
    }

    List<String> dataFieldsToExtract = qSchema.getElementNames();
    for (String fieldName : dataFieldsToExtract)
    {
      int numElements = 1;
      if (dSchema.isArrayElement(fieldName))
      {
        numElements = numArrayElementsToReturn;
      }
      // Decode elements
      for (int i = 0; i < numElements; ++i)
      {
        String type = dSchema.getElementType(fieldName);
        logger.debug("Extracting value for fieldName = " + fieldName + " type = " + type + " partsIndex = " + partsIndex);

        Object element = ((DataPartitioner) dSchema.getPartitionerForElement(fieldName)).fromPartitions(parts, partsIndex, type);

        qrJSON.setMapping(fieldName, element);
        partsIndex += ((DataPartitioner) dSchema.getPartitionerForElement(fieldName)).getNumPartitions(type);

        logger.debug("Adding qrJSON element = " + element + " element.getClass() = " + element.getClass());
      }
    }

    return qrJSON;
  }

  /**
   * Method to convert the given data element given by the JSONObject data element into the extracted BigInteger partitions based upon the given queryType
   */
  public static ArrayList<BigInteger> partitionDataElement(QuerySchema qSchema, JSONObject jsonData, boolean embedSelector) throws Exception
  {
    ArrayList<BigInteger> parts = new ArrayList<>();
    DataSchema dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());

    // Add the embedded selector to the parts
    if (embedSelector)
    {
      String selectorFieldName = qSchema.getSelectorName();
      String type = dSchema.getElementType(selectorFieldName);
      String selector = getSelectorByQueryTypeJSON(qSchema, jsonData);

      parts.addAll(embeddedSelectorToPartitions(selector, type, (dSchema.getPartitionerForElement(selectorFieldName))));

      logger.debug("Added embedded selector for selector = " + selector + " type = " + type + " parts.size() = " + parts.size());
    }

    // Add all appropriate data fields
    List<String> dataFieldsToExtract = qSchema.getElementNames();
    for (String fieldName : dataFieldsToExtract)
    {
      Object dataElement = null;
      if (jsonData.containsKey(fieldName))
      {
        dataElement = jsonData.get(fieldName);
      }

      if (dSchema.isArrayElement(fieldName))
      {
        List<String> elementArray;
        if (dataElement == null)
        {
          elementArray = Collections.singletonList("0");
        }
        else
        {
          elementArray = StringUtils.jsonArrayStringToArrayList(dataElement.toString());
        }
        logger.debug("Adding parts for fieldName = " + fieldName + " type = " + dSchema.getElementType(fieldName) + " jsonData = " + dataElement);

        parts.addAll(((DataPartitioner) dSchema.getPartitionerForElement(fieldName)).arrayToPartitions(elementArray, dSchema.getElementType(fieldName)));
      }
      else
      {
        if (dataElement == null)
        {
          dataElement = "0";
        }
        logger.debug("Adding parts for fieldName = " + fieldName + " type = " + dSchema.getElementType(fieldName) + " jsonData = " + dataElement);

        parts.addAll(((DataPartitioner) dSchema.getPartitionerForElement(fieldName)).toPartitions(dataElement.toString(), dSchema.getElementType(fieldName)));
      }
    }
    logger.debug("parts.size() = " + parts.size());

    return parts;
  }

  /**
   * Method to convert the given data element given by the MapWritable data element into the extracted BigInteger partitions based upon the given queryType
   */
  public static ArrayList<BigInteger> partitionDataElement(MapWritable dataMap, QuerySchema qSchema, DataSchema dSchema, boolean embedSelector)
      throws Exception
  {
    ArrayList<BigInteger> parts = new ArrayList<>();

    logger.debug("queryType = " + qSchema.getSchemaName());

    // Add the embedded selector to the parts
    if (embedSelector)
    {
      String selectorFieldName = qSchema.getSelectorName();
      String type = dSchema.getElementType(selectorFieldName);
      String selector = getSelectorByQueryType(dataMap, qSchema, dSchema);

      parts.addAll(embeddedSelectorToPartitions(selector, type, dSchema.getPartitionerForElement(selectorFieldName)));

      logger.debug("Added embedded selector for selector = " + selector + " parts.size() = " + parts.size());
    }

    // Add all appropriate data fields
    List<String> dataFieldsToExtract = qSchema.getElementNames();
    for (String fieldName : dataFieldsToExtract)
    {
      Object dataElement = null;
      if (dataMap.containsKey(dSchema.getTextName(fieldName)))
      {
        dataElement = dataMap.get(dSchema.getTextName(fieldName));
      }

      if (dSchema.isArrayElement(fieldName))
      {
        List<String> elementArray = null;
        if (dataElement == null)
        {
          elementArray = Collections.singletonList("");
        }
        else if (dataElement instanceof WritableArrayWritable)
        {
          elementArray = Arrays.asList(((WritableArrayWritable) dataElement).toStrings());
        }
        else if (dataElement instanceof ArrayWritable)
        {
          elementArray = Arrays.asList(((ArrayWritable) dataElement).toStrings());
        }

        parts.addAll(((DataPartitioner) dSchema.getPartitionerForElement(fieldName)).arrayToPartitions(elementArray, dSchema.getElementType(fieldName)));
      }
      else
      {
        if (dataElement == null)
        {
          dataElement = "";
        }
        else if (dataElement instanceof Text)
        {
          dataElement = dataElement.toString();
        }
        parts.addAll(((DataPartitioner) dSchema.getPartitionerForElement(fieldName)).toPartitions(dataElement, dSchema.getElementType(fieldName)));
      }
    }
    logger.debug("parts.size() = " + parts.size());

    return parts;
  }

  /**
   * Method to convert the given selector into the extracted BigInteger partitions
   */
  public static ArrayList<BigInteger> embeddedSelectorToPartitions(Object selector, String type, Object partitioner) throws Exception
  {
    ArrayList<BigInteger> parts;

    int partitionBits = ((DataPartitioner) partitioner).getBits(type);
    if (partitionBits > 32) // hash and add 32-bit hash value to partitions
    {
      int hashedSelector = KeyedHash.hash("aux", 32, selector.toString(), "MD5");
      parts = ((DataPartitioner) partitioner).toPartitions(hashedSelector, PrimitiveTypePartitioner.INT);
    }
    else
    // if selector size <= 32 bits or is an IP, add actual selector
    {
      parts = ((DataPartitioner) partitioner).toPartitions(selector, type);
    }

    return parts;
  }

  /**
   * Method get the embedded selector from a given selector
   * 
   */
  public static String getEmbeddedSelector(Object selector, String type, Object partitioner) throws Exception
  {
    String embeddedSelector;

    int partitionBits = ((DataPartitioner) partitioner).getBits(type);
    if (partitionBits > 32) // hash and add 32-bit hash value to partitions
    {
      embeddedSelector = String.valueOf(KeyedHash.hash("aux", 32, selector.toString(), "MD5"));
    }
    else
    // if selector size <= 32 bits, add actual selector
    {
      embeddedSelector = selector.toString();
    }

    return embeddedSelector;
  }

  /**
   * Reconstructs the String version of the embedded selector from its partitions
   */
  public static String getEmbeddedSelectorFromPartitions(ArrayList<BigInteger> parts, int partsIndex, String type, Object partitioner) throws Exception
  {
    String embeddedSelector;

    int partitionBits = ((DataPartitioner) partitioner).getBits(type);
    if (partitionBits > 32) // the embedded selector will be the 32-bit hash value of the hit selector
    {
      embeddedSelector = ((DataPartitioner) partitioner).fromPartitions(parts, partsIndex, PrimitiveTypePartitioner.INT).toString();
    }
    else
    // if selector size <= 32 bits or is an IP, the actual selector was embedded
    {
      embeddedSelector = ((DataPartitioner) partitioner).fromPartitions(parts, partsIndex, type).toString();
    }

    return embeddedSelector;
  }

  /**
   * Pulls the correct selector from the MapWritable data element given the queryType
   * <p>
   * Pulls first element of array if element is an array type
   */
  public static String getSelectorByQueryType(MapWritable dataMap, QuerySchema qSchema, DataSchema dSchema)
  {
    String selector;

    String fieldName = qSchema.getSelectorName();
    if (dSchema.isArrayElement(fieldName))
    {
      if (dataMap.get(dSchema.getTextName(fieldName)) instanceof WritableArrayWritable)
      {
        String[] selectorArray = ((WritableArrayWritable) dataMap.get(dSchema.getTextName(fieldName))).toStrings();
        selector = selectorArray[0];
      }
      else
      {
        String[] elementArray = ((ArrayWritable) (dataMap.get(dSchema.getTextName(fieldName)))).toStrings();
        selector = elementArray[0];
      }
    }
    else
    {
      selector = dataMap.get(dSchema.getTextName(fieldName)).toString();
    }

    return selector;
  }

  /**
   * Pulls the correct selector from the JSONObject data element given the queryType
   * <p>
   * Pulls first element of array if element is an array type
   */
  public static String getSelectorByQueryTypeJSON(QuerySchema qSchema, JSONObject dataMap)
  {
    String selector;

    DataSchema dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());
    String fieldName = qSchema.getSelectorName();

    if (dSchema.isArrayElement(fieldName))
    {
      ArrayList<String> elementArray = StringUtils.jsonArrayStringToArrayList(dataMap.get(fieldName).toString());
      selector = elementArray.get(0);
    }
    else
    {
      selector = dataMap.get(fieldName).toString();
    }
    return selector;
  }

  // For debug
  private static void printParts(ArrayList<BigInteger> parts)
  {
    int i = 0;
    for (BigInteger part : parts)
    {
      logger.debug("parts(" + i + ") = " + parts.get(i).intValue() + " parts bits = " + parts.get(i).toString(2));
      ++i;
    }
  }
}
