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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.pirk.encryption.ModPowAbstraction;
import org.apache.pirk.querier.wideskies.encrypt.ExpTableRunnable;
import org.apache.pirk.serialization.Storable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold the PIR query vectors
 *
 */
public class Query implements Serializable, Storable
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(Query.class);

  private QueryInfo qInfo = null; // holds all query info

  private TreeMap<Integer,BigInteger> queryElements = null; // query elements - ordered on insertion

  // lookup table for exponentiation of query vectors - based on dataPartitionBitSize
  // element -> <power, element^power mod N^2>
  private HashMap<BigInteger,HashMap<Integer,BigInteger>> expTable = null;

  // File based lookup table for modular exponentiation
  // element hash -> filename containing it's <power, element^power mod N^2> modular exponentiations
  private HashMap<Integer,String> expFileBasedLookup = null;

  private BigInteger N = null; // N=pq, RSA modulus for the Paillier encryption associated with the queryElements
  private BigInteger NSquared = null;

  public Query(QueryInfo queryInfoIn, BigInteger NInput)
  {
    qInfo = queryInfoIn;
    N = NInput;
    NSquared = N.pow(2);

    queryElements = new TreeMap<>();
    expTable = new HashMap<>();

    expFileBasedLookup = new HashMap<>();
  }

  public QueryInfo getQueryInfo()
  {
    return qInfo;
  }

  public TreeMap<Integer,BigInteger> getQueryElements()
  {
    return queryElements;
  }

  public BigInteger getQueryElement(int index)
  {
    return queryElements.get(index);
  }

  public BigInteger getN()
  {
    return N;
  }

  public BigInteger getNSquared()
  {
    return NSquared;
  }

  public HashMap<Integer,String> getExpFileBasedLookup()
  {
    return expFileBasedLookup;
  }

  public String getExpFile(int i)
  {
    return expFileBasedLookup.get(i);
  }

  public void setExpFileBasedLookup(HashMap<Integer,String> expInput)
  {
    expFileBasedLookup = expInput;
  }

  public HashMap<BigInteger,HashMap<Integer,BigInteger>> getExpTable()
  {
    return expTable;
  }

  public void setExpTable(HashMap<BigInteger,HashMap<Integer,BigInteger>> expTableInput)
  {
    expTable = expTableInput;
  }

  public void addQueryElement(Integer index, BigInteger element)
  {
    queryElements.put(index, element);
  }

  public void addQueryElements(TreeMap<Integer,BigInteger> elements)
  {
    queryElements.putAll(elements);
  }

  public boolean containsElement(BigInteger element)
  {
    return queryElements.containsValue(element);
  }

  public void clearElements()
  {
    queryElements.clear();
  }

  /**
   * This should be called after all query elements have been added in order to generate the expTable. For int exponentiation with BigIntegers, assumes that
   * dataPartitionBitSize < 32.
   *
   */
  public void generateExpTable(int numThreads) throws InterruptedException
  {
    int dataPartitionBitSize = qInfo.getDataPartitionBitSize();
    int maxValue = (int) Math.pow(2, dataPartitionBitSize) - 1;

    if (numThreads < 2)
    {
      for (BigInteger element : queryElements.values())
      {
        logger.debug("element = " + element.toString(2) + " maxValue = " + maxValue + " dataPartitionBitSize = " + dataPartitionBitSize);

        HashMap<Integer,BigInteger> powMap = new HashMap<>(); // <power, element^power mod N^2>
        for (int i = 0; i <= maxValue; ++i)
        {
          BigInteger value = ModPowAbstraction.modPow(element, BigInteger.valueOf(i), NSquared);

          powMap.put(i, value);
        }
        expTable.put(element, powMap);
      }
    }
    else
    // multithreaded case
    {
      ExecutorService es = Executors.newCachedThreadPool();
      int elementsPerThread = queryElements.size() / numThreads; // Integral division.

      ArrayList<ExpTableRunnable> runnables = new ArrayList<>();
      for (int i = 0; i < numThreads; ++i)
      {
        // Grab the range of the thread and create the corresponding partition of selectors
        int start = i * elementsPerThread;
        int stop = start + elementsPerThread - 1;
        if (i == (numThreads - 1))
        {
          stop = queryElements.size() - 1;
        }
        ArrayList<BigInteger> queryElementsPartition = new ArrayList<>();
        for (int j = start; j <= stop; ++j)
        {
          queryElementsPartition.add(queryElements.get(j));
        }

        // Create the runnable and execute
        // selectorMaskMap and rElements are synchronized, pirWatchlist is copied, selectors is partitioned
        ExpTableRunnable pirExpRun = new ExpTableRunnable(dataPartitionBitSize, NSquared, queryElementsPartition);

        runnables.add(pirExpRun);
        es.execute(pirExpRun);
      }

      // Allow threads to complete
      es.shutdown(); // previously submitted tasks are executed, but no new tasks will be accepted
      boolean finished = es.awaitTermination(1, TimeUnit.DAYS); // waits until all tasks complete or until the specified timeout
      if (!finished)
      {
        throw new InterruptedException("Operation timed out.");
      }

      // Pull all decrypted elements and add to resultMap
      for (ExpTableRunnable runner : runnables)
      {
        HashMap<BigInteger,HashMap<Integer,BigInteger>> expValues = runner.getExpTable();
        expTable.putAll(expValues);
      }
      logger.debug("expTable.size() = " + expTable.keySet().size() + " NSqaured = " + NSquared.intValue() + " = " + NSquared.toString());
      for (Entry<BigInteger,HashMap<Integer,BigInteger>> entry : expTable.entrySet())
      {
        logger.debug("expTable for key = " + entry.getKey().toString() + " = " + entry.getValue().size());
      }
    }
  }

  public BigInteger getExp(BigInteger value, int power)
  {
    return expTable.get(value).get(power);
  }
}
