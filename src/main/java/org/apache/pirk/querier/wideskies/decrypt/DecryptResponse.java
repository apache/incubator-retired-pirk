/*******************************************************************************
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
 *******************************************************************************/
package org.apache.pirk.querier.wideskies.decrypt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.querier.wideskies.Querier;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.utils.LogUtils;
import org.apache.pirk.utils.PIRException;

/**
 * Class to perform PIR decryption
 * 
 */
public class DecryptResponse
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  Response response = null;

  Querier querier = null;

  HashMap<String,ArrayList<QueryResponseJSON>> resultMap = null; // selector -> ArrayList of hits

  public DecryptResponse(Response responseInput, Querier querierInput)
  {
    response = responseInput;
    querier = querierInput;

    resultMap = new HashMap<String,ArrayList<QueryResponseJSON>>();
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
    ArrayList<String> selectors = querier.getSelectors();
    HashMap<Integer,String> embedSelectorMap = querier.getEmbedSelectorMap();

    // Perform decryption on the encrypted columns
    ArrayList<BigInteger> rElements = decryptElements(response.getResponseElements(), paillier);
    logger.debug("rElements.size() = " + rElements.size());

    // Pull the necessary parameters
    int dataPartitionBitSize = queryInfo.getDataPartitionBitSize();

    // Initialize the result map and masks-- removes initialization checks from code below
    HashMap<String,BigInteger> selectorMaskMap = new HashMap<String,BigInteger>();
    int selectorNum = 0;
    BigInteger twoBI = BigInteger.valueOf(2);
    for (String selector : selectors)
    {
      resultMap.put(selector, new ArrayList<QueryResponseJSON>());

      // 2^{selectorNum*dataPartitionBitSize}(2^{dataPartitionBitSize} - 1)
      BigInteger mask = twoBI.pow(selectorNum * dataPartitionBitSize).multiply((twoBI.pow(dataPartitionBitSize).subtract(BigInteger.ONE)));
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
    int elementsPerThread = (int) (Math.floor(selectors.size() / numThreads));

    ArrayList<DecryptResponseRunnable> runnables = new ArrayList<DecryptResponseRunnable>();
    for (int i = 0; i < numThreads; ++i)
    {
      // Grab the range of the thread and create the corresponding partition of selectors
      int start = i * elementsPerThread;
      int stop = start + elementsPerThread - 1;
      if (i == (numThreads - 1))
      {
        stop = selectors.size() - 1;
      }
      TreeMap<Integer,String> selectorsPartition = new TreeMap<Integer,String>();
      for (int j = start; j <= stop; ++j)
      {
        selectorsPartition.put(j, selectors.get(j));
      }

      // Create the runnable and execute
      // selectorMaskMap and rElements are synchronized, pirWatchlist is copied, selectors is partitioned
      DecryptResponseRunnable runDec = new DecryptResponseRunnable(rElements, selectorsPartition, selectorMaskMap, queryInfo.copy(), embedSelectorMap);
      runnables.add(runDec);
      es.execute(runDec);
    }

    // Allow threads to complete
    es.shutdown(); // previously submitted tasks are executed, but no new tasks will be accepted
    boolean finished = es.awaitTermination(1, TimeUnit.DAYS); // waits until all tasks complete or until the specified timeout

    if (!finished)
    {
      throw new PIRException("Decryption threads did not finish in the alloted time");
    }

    // Pull all decrypted elements and add to resultMap
    for (DecryptResponseRunnable runner : runnables)
    {
      HashMap<String,ArrayList<QueryResponseJSON>> decValues = runner.getResultMap();
      resultMap.putAll(decValues);
    }
  }

  // Method to perform basic decryption of each raw response element - does not
  // extract and reconstruct the data elements
  private ArrayList<BigInteger> decryptElements(TreeMap<Integer,BigInteger> elements, Paillier paillier)
  {
    ArrayList<BigInteger> decryptedElements = new ArrayList<BigInteger>();

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
    FileOutputStream fout = new FileOutputStream(new File(filename));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout));
    for (String selector : resultMap.keySet())
    {
      ArrayList<QueryResponseJSON> hits = resultMap.get(selector);
      for (QueryResponseJSON hitJSON : hits)
      {
        bw.write(hitJSON.getJSONString());
        bw.newLine();
      }
    }
    bw.close();
  }

  /**
   * Writes elements of the resultMap to output file, one line for each element, where each line is a string representation of the corresponding
   * QueryResponseJSON object
   */
  public void writeResultFile(File file) throws IOException
  {
    FileOutputStream fout = new FileOutputStream(file);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout));
    for (String selector : resultMap.keySet())
    {
      ArrayList<QueryResponseJSON> hits = resultMap.get(selector);
      for (QueryResponseJSON hitJSON : hits)
      {
        bw.write(hitJSON.getJSONString());
        bw.newLine();
      }
    }
    bw.close();
  }
}
