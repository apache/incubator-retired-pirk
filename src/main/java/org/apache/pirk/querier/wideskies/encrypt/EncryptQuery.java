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
package org.apache.pirk.querier.wideskies.encrypt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.querier.wideskies.Querier;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.query.wideskies.QueryUtils;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaRegistry;
import org.apache.pirk.utils.KeyedHash;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to perform PIR encryption
 *
 */
public class EncryptQuery
{
  private static final Logger logger = LoggerFactory.getLogger(EncryptQuery.class);

  private QueryInfo queryInfo = null; // contains basic query information and functionality

  private Query query = null; // contains the query vectors

  private Querier querier = null; // contains the query vectors and encryption object

  private Paillier paillier = null; // Paillier encryption functionality

  private ArrayList<String> selectors = null; // selectors for the query

  // Map to check the embedded selectors in the results for false positives;
  // if the selector is a fixed size < 32 bits, it is included as is
  // if the selector is of variable lengths
  private HashMap<Integer,String> embedSelectorMap = null;

  public EncryptQuery(QueryInfo queryInfoInput, ArrayList<String> selectorsInput, Paillier paillierInput)
  {
    queryInfo = queryInfoInput;

    selectors = selectorsInput;

    paillier = paillierInput;

    embedSelectorMap = new HashMap<>();
  }

  public Paillier getPaillier()
  {
    return paillier;
  }

  public QueryInfo getQueryInfo()
  {
    return queryInfo;
  }

  public Query getQuery()
  {
    return query;
  }

  public Querier getQuerier()
  {
    return querier;
  }

  public ArrayList<String> getSelectors()
  {
    return selectors;
  }

  public HashMap<Integer,String> getEmbedSelectorMap()
  {
    return embedSelectorMap;
  }

  /**
   * Encrypt, building the Query object, calculating and setting the query vectors.
   * <p>
   * Uses the system configured number of threads to conduct the encryption, or a single thread if the configuration has not been set.
   * 
   * @throws InterruptedException
   *           if the task was interrupted during encryption.
   * @throws PIRException
   */
  public void encrypt() throws InterruptedException, PIRException
  {
    int numThreads = Integer.parseInt(SystemConfiguration.getProperty("numThreads", "1"));
    encrypt(numThreads);
  }

  /**
   * Encrypt, building the Query object, calculating and setting the query vectors
   * <p>
   * If we have hash collisions over our selector set, we will append integers to the key starting with 0 until we no longer have collisions.
   * <p>
   * For encrypted query vector E = <E_0, ..., E_{(2^hashBitSize)-1}>:
   * <p>
   * E_i = 2^{j*dataPartitionBitSize} if i = H_k(selector_j) 0 otherwise
   */
  public void encrypt(int numThreads) throws InterruptedException, PIRException
  {
    query = new Query(queryInfo, paillier.getN());

    // Determine the query vector mappings for the selectors; vecPosition -> selectorNum
    HashMap<Integer,Integer> selectorQueryVecMapping = computeSelectorQueryVecMap();

    // Form the embedSelectorMap
    populateEmbeddedSelectorMap();

    if (numThreads == 1)
    {
      serialEncrypt(selectorQueryVecMapping);
    }
    else
    {
      parallelEncrypt(Math.max(2, numThreads), selectorQueryVecMapping);
    }

    // Generate the expTable in Query, if we are using it and if
    // useHDFSExpLookupTable is false -- if we are generating it as standalone and not on the cluster
    if (query.getQueryInfo().getUseExpLookupTable() && !query.getQueryInfo().getUseHDFSExpLookupTable())
    {
      logger.info("Starting expTable generation");

      // This has to be reasonably multithreaded or it takes forever...
      query.generateExpTable(Math.max(8, numThreads));
    }

    // Set the Querier object
    querier = new Querier(queryInfo, selectors, paillier, query, embedSelectorMap);
  }

  private HashMap<Integer,Integer> computeSelectorQueryVecMap()
  {
    String hashKey = queryInfo.getHashKey();
    int keyCounter = 0;
    int numSelectors = selectors.size();
    HashSet<Integer> hashes = new HashSet<>(numSelectors);
    HashMap<Integer,Integer> selectorQueryVecMapping = new HashMap<>(numSelectors);

    for (int index = 0; index < numSelectors; index++)
    {
      String selector = selectors.get(index);
      int hash = KeyedHash.hash(hashKey, queryInfo.getHashBitSize(), selector);

      // All keyed hashes of the selectors must be unique
      if (hashes.add(hash))
      {
        // The hash is unique
        selectorQueryVecMapping.put(hash, index);
        logger.debug("index = " + index + "selector = " + selector + " hash = " + hash);
      }
      else
      {
        // Hash collision
        hashes.clear();
        selectorQueryVecMapping.clear();
        hashKey = queryInfo.getHashKey() + ++keyCounter;
        logger.debug("index = " + index + "selector = " + selector + " hash collision = " + hash + " new key = " + hashKey);
        index = 0;
      }
    }
    return selectorQueryVecMapping;
  }

  private void populateEmbeddedSelectorMap()
  {
    QuerySchema qSchema = QuerySchemaRegistry.get(queryInfo.getQueryType());
    DataSchema dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());
    String type = dSchema.getElementType(qSchema.getSelectorName());
    int sNum = 0;
    for (String selector : selectors)
    {
      String embeddedSelector = null;
      try
      {
        embeddedSelector = QueryUtils.getEmbeddedSelector(selector, type, dSchema.getPartitionerForElement(qSchema.getSelectorName()));
      } catch (Exception e)
      {
        logger.info("Caught exception for selector = " + selector);
        e.printStackTrace();
        // TODO: Check: should continue?
      }

      embedSelectorMap.put(sNum, embeddedSelector);
      ++sNum;
    }
  }

  private void serialEncrypt(HashMap<Integer,Integer> selectorQueryVecMapping) throws PIRException
  {
    int numElements = 1 << queryInfo.getHashBitSize(); // 2^hashBitSize

    EncryptQueryRunnable runner = new EncryptQueryRunnable(queryInfo.getDataPartitionBitSize(), paillier, selectorQueryVecMapping, 0, numElements - 1);
    runner.run();

    query.addQueryElements(runner.getEncryptedValues());

    logger.info("Completed serial creation of encrypted query vectors");
  }

  private void parallelEncrypt(int numThreads, HashMap<Integer,Integer> selectorQueryVecMapping) throws PIRException
  {
    // Encrypt and form the query vector
    ExecutorService es = Executors.newCachedThreadPool();
    ArrayList<EncryptQueryRunnable> runnables = new ArrayList<>(numThreads);
    int numElements = 1 << queryInfo.getHashBitSize(); // 2^hashBitSize

    // Split the work across the requested number of threads
    int elementsPerThread = numElements / numThreads;
    for (int i = 0; i < numThreads; ++i)
    {
      // Grab the range of the thread
      int start = i * elementsPerThread;
      int stop = start + elementsPerThread - 1;
      if (i == (numThreads - 1))
      {
        stop = numElements - 1;
      }

      // Copy selectorQueryVecMapping so we don't have to synchronize - only has size = selectors.size()
      HashMap<Integer,Integer> selectorQueryVecMappingCopy = new HashMap<>(selectorQueryVecMapping);

      // Create the runnable and execute
      EncryptQueryRunnable runEnc = new EncryptQueryRunnable(queryInfo.getDataPartitionBitSize(), paillier.clone(), selectorQueryVecMappingCopy, start, stop);
      runnables.add(runEnc);
      es.execute(runEnc);
    }

    // Allow threads to complete
    es.shutdown(); // previously submitted tasks are executed, but no new tasks will be accepted
    boolean finished = false;
    try
    {
      // waits until all tasks complete or until the specified timeout
      finished = es.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e)
    {
      Thread.interrupted();
    }

    if (!finished)
    {
      throw new PIRException("Encryption threads did not finish in the alloted time");
    }

    // Pull all encrypted elements and add to Query
    for (EncryptQueryRunnable runner : runnables)
    {
      query.addQueryElements(runner.getEncryptedValues());
    }
    logger.info("Completed parallel creation of encrypted query vectors");
  }
}
