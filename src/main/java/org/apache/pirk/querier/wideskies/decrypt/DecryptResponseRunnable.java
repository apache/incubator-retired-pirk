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
package org.apache.pirk.querier.wideskies.decrypt;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.query.wideskies.QueryUtils;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaRegistry;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable class for multithreaded PIR decryption
 * <p>
 * NOTE: rElements and selectorMaskMap are joint access objects, for now
 *
 */
public class DecryptResponseRunnable implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(DecryptResponseRunnable.class);

  private HashMap<String,ArrayList<QueryResponseJSON>> resultMap = null; // selector -> ArrayList of hits

  private ArrayList<BigInteger> rElements = null;
  private TreeMap<Integer,String> selectors = null;
  private HashMap<String,BigInteger> selectorMaskMap = null;
  private QueryInfo queryInfo = null;
  private QuerySchema qSchema = null;

  private HashMap<Integer,String> embedSelectorMap = null;

  public DecryptResponseRunnable(ArrayList<BigInteger> rElementsInput, TreeMap<Integer,String> selectorsInput, HashMap<String,BigInteger> selectorMaskMapInput,
      QueryInfo queryInfoInput, HashMap<Integer,String> embedSelectorMapInput)
  {
    rElements = rElementsInput;
    selectors = selectorsInput;
    selectorMaskMap = selectorMaskMapInput;
    queryInfo = queryInfoInput;
    embedSelectorMap = embedSelectorMapInput;

    if (SystemConfiguration.getProperty("pir.allowAdHocQuerySchemas", "false").equals("true"))
    {
      if ((qSchema = queryInfo.getQuerySchema()) == null)
      {
        qSchema = QuerySchemaRegistry.get(queryInfo.getQueryType());
      }
    }
    resultMap = new HashMap<>();
  }

  public HashMap<String,ArrayList<QueryResponseJSON>> getResultMap()
  {
    return resultMap;
  }

  @Override
  public void run()
  {
    // Pull the necessary parameters
    int dataPartitionBitSize = queryInfo.getDataPartitionBitSize();
    int numPartitionsPerDataElement = queryInfo.getNumPartitionsPerDataElement();

    QuerySchema qSchema = QuerySchemaRegistry.get(queryInfo.getQueryType());
    String selectorName = qSchema.getSelectorName();

    // Initialize - removes checks below
    for (String selector : selectors.values())
    {
      resultMap.put(selector, new ArrayList<QueryResponseJSON>());
    }

    logger.debug("numResults = " + rElements.size() + " numPartitionsPerDataElement = " + numPartitionsPerDataElement);

    // Pull the hits for each selector
    int hits = 0;
    int maxHitsPerSelector = rElements.size() / numPartitionsPerDataElement; // Max number of data hits in the response elements for a given selector
    logger.debug("numHits = " + maxHitsPerSelector);
    while (hits < maxHitsPerSelector)
    {
      int selectorIndex = selectors.firstKey();
      while (selectorIndex <= selectors.lastKey())
      {
        String selector = selectors.get(selectorIndex);
        logger.debug("selector = " + selector);

        ArrayList<BigInteger> parts = new ArrayList<>();
        int partNum = 0;
        boolean zeroElement = true;
        while (partNum < numPartitionsPerDataElement)
        {
          BigInteger part = (rElements.get(hits * numPartitionsPerDataElement + partNum)).and(selectorMaskMap.get(selector)); // pull off the correct bits

          logger.debug("rElements.get(" + (hits * numPartitionsPerDataElement + partNum) + ") = "
              + rElements.get(hits * numPartitionsPerDataElement + partNum).toString(2) + " bitLength = "
              + rElements.get(hits * numPartitionsPerDataElement + partNum).bitLength() + " val = "
              + rElements.get(hits * numPartitionsPerDataElement + partNum));
          logger.debug("colNum = " + (hits * numPartitionsPerDataElement + partNum) + " partNum = " + partNum + " part = " + part);

          part = part.shiftRight(selectorIndex * dataPartitionBitSize);
          parts.add(part);

          logger.debug("partNum = " + partNum + " part = " + part.intValue());

          if (zeroElement)
          {
            if (!part.equals(BigInteger.ZERO))
            {
              zeroElement = false;
            }
          }
          ++partNum;
        }

        logger.debug("parts.size() = " + parts.size());

        if (!zeroElement)
        {
          // Convert biHit to the appropriate QueryResponseJSON object, based on the queryType
          QueryResponseJSON qrJOSN = null;
          try
          {
            qrJOSN = QueryUtils.extractQueryResponseJSON(queryInfo, qSchema, parts);
          } catch (Exception e)
          {
            e.printStackTrace();
          }
          qrJOSN.setMapping(selectorName, selector);
          logger.debug("selector = " + selector + " qrJOSN = " + qrJOSN.getJSONString());

          // Add the hit for this selector - if we are using embedded selectors, check to make sure
          // that the hit's embedded selector in the qrJOSN and the once in the embedSelectorMap match
          boolean addHit = true;
          if (queryInfo.getEmbedSelector())
          {
            if (!(embedSelectorMap.get(selectorIndex)).equals(qrJOSN.getValue(QueryResponseJSON.SELECTOR)))
            {
              addHit = false;
              logger.debug("qrJOSN embedded selector = " + qrJOSN.getValue(QueryResponseJSON.SELECTOR) + " != original embedded selector = "
                  + embedSelectorMap.get(selectorIndex));
            }
          }
          if (addHit)
          {
            ArrayList<QueryResponseJSON> selectorHitList = resultMap.get(selector);
            selectorHitList.add(qrJOSN);
            resultMap.put(selector, selectorHitList);

            // Add the selector into the wlJSONHit
            qrJOSN.setMapping(QueryResponseJSON.SELECTOR, selector);
          }
        }

        ++selectorIndex;
      }
      ++hits;
    }
  }
}
