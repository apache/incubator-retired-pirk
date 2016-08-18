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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.querier.wideskies.Querier;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.utils.PIRException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to perform PIR decryption
 */
public class DecryptResponse
{
  private static final Logger logger = LoggerFactory.getLogger(DecryptResponse.class);
  
  private static final BigInteger TWO_BI = BigInteger.valueOf(2);

  private final Response response;

  private final Querier querier;

  private final Map<String,List<QueryResponseJSON>> resultMap = new HashMap<>(); // selector -> ArrayList of hits

  public DecryptResponse(Response responseInput, Querier querierInput)
  {
    response = responseInput;
    querier = querierInput;
  }

  /**
   * Method to decrypt the response elements and reconstructs the data elements
   * <p>
   * Each element of response.getResponseElements() is an encrypted column vector E(Y_i)
   * <p>
   * To decrypt and recover data elements:
   * <p>
   * (1) Decrypt E(Y_i) to yield
   * <p>
   * Y_i, where Y_i = \sum_{j = 0}^{numSelectors} 2^{j*dataPartitionBitSize} D_j
   * <p>
   * such that D_j is dataPartitionBitSize-many bits of data corresponding to selector_k for j = H_k(selector_k), for some 0 <= k < numSelectors
   * <p>
   * (2) Reassemble data elements across columns where, hit r for selector_k, D^k_r, is such that
   * <p>
   * D^k_r = D^k_r,0 || D^k_r,1 || ... || D^k_r,(numPartitionsPerDataElement - 1)
   * <p>
   * where D^k_r,l = Y_{r*numPartitionsPerDataElement + l} & (2^{r*numPartitionsPerDataElement} * (2^numBitsPerDataElement - 1))
   *
   */
  public void decrypt(int numThreads) throws InterruptedException, PIRException
  {
    QueryInfo queryInfo = response.getQueryInfo();

    Paillier paillier = querier.getPaillier();
    List<String> selectors = querier.getSelectors();
    Map<Integer,String> embedSelectorMap = querier.getEmbedSelectorMap();

    // Perform decryption on the encrypted columns
    List<BigInteger> rElements = decryptElements(response.getResponseElements(), paillier);
    logger.debug("rElements.size() = " + rElements.size());

    // Pull the necessary parameters
    int dataPartitionBitSize = queryInfo.getDataPartitionBitSize();

    // Initialize the result map and masks-- removes initialization checks from code below
    Map<String,BigInteger> selectorMaskMap = new HashMap<>();
    int selectorNum = 0;
    for (String selector : selectors)
    {
      resultMap.put(selector, new ArrayList<>());

      // 2^{selectorNum*dataPartitionBitSize}(2^{dataPartitionBitSize} - 1)
      BigInteger mask = TWO_BI.pow(selectorNum * dataPartitionBitSize).multiply((TWO_BI.pow(dataPartitionBitSize).subtract(BigInteger.ONE)));
      logger.debug("selector = " + selector + " mask = " + mask.toString(2));
      selectorMaskMap.put(selector, mask);

      ++selectorNum;
    }

    // Decrypt via Runnables
    ExecutorService es = Executors.newCachedThreadPool();
    if (selectors.size() < numThreads)
    {
      numThreads = selectors.size();
    }
    int elementsPerThread = selectors.size() / numThreads; // Integral division.

    List<Future<Map<String,List<QueryResponseJSON>>>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; ++i)
    {
      // Grab the range of the thread and create the corresponding partition of selectors
      int start = i * elementsPerThread;
      int stop = start + elementsPerThread - 1;
      if (i == (numThreads - 1))
      {
        stop = selectors.size() - 1;
      }
      TreeMap<Integer,String> selectorsPartition = new TreeMap<>();
      for (int j = start; j <= stop; ++j)
      {
        selectorsPartition.put(j, selectors.get(j));
      }

      // Create the runnable and execute
      DecryptResponseRunnable<Map<String,List<QueryResponseJSON>>> runDec = new DecryptResponseRunnable<>(rElements, selectorsPartition, selectorMaskMap, queryInfo.clone(), embedSelectorMap);
      futures.add(es.submit(runDec));
    }

    // Pull all decrypted elements and add to resultMap
    try
    {
      for (Future<Map<String,List<QueryResponseJSON>>> future : futures)
      {
        resultMap.putAll(future.get(1, TimeUnit.DAYS));
      }
    } catch (TimeoutException | ExecutionException e)
    {
      throw new PIRException("Exception in decryption threads.", e);
    }
    
    es.shutdown();
  }

  // Method to perform basic decryption of each raw response element - does not
  // extract and reconstruct the data elements
  private List<BigInteger> decryptElements(TreeMap<Integer,BigInteger> elements, Paillier paillier)
  {
    List<BigInteger> decryptedElements = new ArrayList<>();

    for (BigInteger encElement : elements.values())
    {
      decryptedElements.add(paillier.decrypt(encElement));
    }

    return decryptedElements;
  }

  /**
   * Writes elements of the resultMap to output file, one line for each element, where each line is a string representation of the corresponding
   * QueryResponseJSON object
   */
  public void writeResultFile(String filename) throws IOException
  {
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filename))))
    {
      for (Entry<String,List<QueryResponseJSON>> entry : resultMap.entrySet())
      {
        for (QueryResponseJSON hitJSON : entry.getValue())
        {
          bw.write(hitJSON.getJSONString());
          bw.newLine();
        }
      }
    }
  }

  /**
   * Writes elements of the resultMap to output file, one line for each element, where each line is a string representation of the corresponding
   * QueryResponseJSON object
   */
  public void writeResultFile(File file) throws IOException
  {
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file)))
    {
      for (Entry<String,List<QueryResponseJSON>> entry : resultMap.entrySet())
      {
        for (QueryResponseJSON hitJSON : entry.getValue())
        {
          bw.write(hitJSON.getJSONString());
          bw.newLine();
        }
      }
    }
  }
}
