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

import org.apache.commons.codec.binary.Hex;
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
import org.apache.pirk.utils.RandomProvider;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Class to perform PIR encryption
 */
public class EncryptQuery
{
  private static final Logger logger = LoggerFactory.getLogger(EncryptQuery.class);

  // Contains basic query information.
  private final QueryInfo queryInfo;

  // Selectors for this query.
  private final List<String> selectors;

  // Paillier encryption functionality.
  private final Paillier paillier;

  /**
   * Constructs a query encryptor using the given query information, selectors, and Paillier cryptosystem.
   *
   * @param queryInfo Fundamental information about the query.
   * @param selectors the list of selectors for this query.
   * @param paillier  the Paillier cryptosystem to use.
   */
  public EncryptQuery(QueryInfo queryInfo, List<String> selectors, Paillier paillier)
  {
    this.queryInfo = queryInfo;
    this.selectors = selectors;
    this.paillier = paillier;
  }

  /**
   * Encrypts the query described by the query information using Paillier encryption.
   * <p>
   * The encryption builds a <code>Querier</code> object, calculating and setting the query vectors.
   * <p>
   * Uses the system configured number of threads to conduct the encryption, or a single thread if the configuration has not been set.
   *
   * @return The querier containing the query, and all information required to perform decryption.
   * @throws InterruptedException If the task was interrupted during encryption.
   * @throws PIRException         If a problem occurs performing the encryption.
   */
  public Querier encrypt() throws InterruptedException, PIRException
  {
    int numThreads = SystemConfiguration.getIntProperty("numThreads", 1);
    return encrypt(numThreads);
  }

  /**
   * Encrypts the query described by the query information using Paillier encryption using the given number of threads.
   * <p>
   * The encryption builds a <code>Querier</code> object, calculating and setting the query vectors.
   * <p>
   * If we have hash collisions over our selector set, we will append integers to the key starting with 0 until we no longer have collisions.
   * <p>
   * For encrypted query vector E = <E_0, ..., E_{(2^hashBitSize)-1}>:
   * <p>
   * E_i = 2^{j*dataPartitionBitSize} if i = H_k(selector_j) 0 otherwise
   *
   * @param numThreads the number of threads to use when performing the encryption.
   * @return The querier containing the query, and all information required to perform decryption.
   * @throws InterruptedException If the task was interrupted during encryption.
   * @throws PIRException         If a problem occurs performing the encryption.
   */
  public Querier encrypt(int numThreads) throws InterruptedException, PIRException
  {
    // Determine the query vector mappings for the selectors; vecPosition -> selectorNum
    Map<Integer,Integer> selectorQueryVecMapping = computeSelectorQueryVecMap();

    // Form the embedSelectorMap
    // Map to check the embedded selectors in the results for false positives;
    // if the selector is a fixed size < 32 bits, it is included as is
    // if the selector is of variable lengths
    Map<Integer,String> embedSelectorMap = computeEmbeddedSelectorMap();

    SortedMap<Integer,BigInteger> queryElements;
    if (numThreads == 1)
    {
      queryElements = serialEncrypt(selectorQueryVecMapping);
      logger.info("Completed serial creation of encrypted query vectors");
    }
    else
    {
      queryElements = parallelEncrypt(selectorQueryVecMapping, Math.max(2, numThreads));
      logger.info("Completed parallel creation of encrypted query vectors");
    }

    Query query = new Query(queryInfo, paillier.getN(), queryElements);

    // Generate the expTable in Query, if we are using it and if
    // useHDFSExpLookupTable is false -- if we are generating it as standalone and not on the cluster
    if (queryInfo.useExpLookupTable() && !queryInfo.useHDFSExpLookupTable())
    {
      logger.info("Starting expTable generation");
      query.generateExpTable();
    }

    // Return the Querier object.
    return new Querier(selectors, paillier, query, embedSelectorMap);
  }

  /**
   * Use this method to get a securely generated, random string of 2*numBytes length
   *
   * @param numBytes How many bytes of random data to return.
   * @return Random hex string of 2*numBytes length
   */
  private String getRandByteString(int numBytes)
  {
    byte[] randomData = new byte[numBytes];
    RandomProvider.SECURE_RANDOM.nextBytes(randomData);
    return Hex.encodeHexString(randomData);
  }

  private Map<Integer,Integer> computeSelectorQueryVecMap()
  {
    String hashKey = getRandByteString(10);
    int numSelectors = selectors.size();
    Map<Integer,Integer> selectorQueryVecMapping = new HashMap<>(numSelectors);

    for (int index = 0; index < numSelectors; index++)
    {
      String selector = selectors.get(index);
      int hash = KeyedHash.hash(hashKey, queryInfo.getHashBitSize(), selector);

      // All keyed hashes of the selectors must be unique
      if (selectorQueryVecMapping.put(hash, index) == null)
      {
        // The hash is unique
        logger.debug("index = " + index + "selector = " + selector + " hash = " + hash);
      }
      else
      {
        // Hash collision
        selectorQueryVecMapping.clear();
        hashKey = getRandByteString(10);
        logger.debug("index = " + index + "selector = " + selector + " hash collision = " + hash + " new key = " + hashKey);
        index = -1;
      }
    }

    // Save off the final hashKey that we ended up using
    queryInfo.setHashKey(hashKey);
    return selectorQueryVecMapping;
  }

  private Map<Integer,String> computeEmbeddedSelectorMap() throws PIRException
  {
    QuerySchema qSchema = QuerySchemaRegistry.get(queryInfo.getQueryType());
    String selectorName = qSchema.getSelectorName();
    DataSchema dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());
    String type = dSchema.getElementType(selectorName);

    Map<Integer,String> embedSelectorMap = new HashMap<>(selectors.size());

    int sNum = 0;
    for (String selector : selectors)
    {
      String embeddedSelector = QueryUtils.getEmbeddedSelector(selector, type, dSchema.getPartitionerForElement(selectorName));
      embedSelectorMap.put(sNum, embeddedSelector);
      sNum += 1;
    }

    return embedSelectorMap;
  }

  /*
   * Perform the encryption using a single thread, avoiding the overhead of thread management.
   */
  private SortedMap<Integer,BigInteger> serialEncrypt(Map<Integer,Integer> selectorQueryVecMapping) throws PIRException
  {
    int numElements = 1 << queryInfo.getHashBitSize(); // 2^hashBitSize

    EncryptQueryTask task = new EncryptQueryTask(queryInfo.getDataPartitionBitSize(), paillier, selectorQueryVecMapping, 0, numElements - 1);

    return task.call();
  }

  /*
   * Performs the encryption with numThreads.
   */
  private SortedMap<Integer,BigInteger> parallelEncrypt(Map<Integer,Integer> selectorQueryVecMapping, int numThreads) throws InterruptedException, PIRException
  {
    // Encrypt and form the query vector
    ExecutorService es = Executors.newCachedThreadPool();
    List<Future<SortedMap<Integer,BigInteger>>> futures = new ArrayList<>(numThreads);
    int numElements = 1 << queryInfo.getHashBitSize(); // 2^hashBitSize

    // Split the work across the requested number of threads
    int elementsPerThread = numElements / numThreads;
    for (int i = 0; i < numThreads; ++i)
    {
      // Grab the range for this thread
      int start = i * elementsPerThread;
      int stop = start + elementsPerThread - 1;
      if (i == numThreads - 1)
      {
        stop = numElements - 1;
      }

      // Create the runnable and execute
      EncryptQueryTask runEnc = new EncryptQueryTask(queryInfo.getDataPartitionBitSize(), paillier, selectorQueryVecMapping, start, stop);
      futures.add(es.submit(runEnc));
    }

    // Pull all encrypted elements and add to resultMap
    SortedMap<Integer,BigInteger> queryElements = new TreeMap<>();
    try
    {
      for (Future<SortedMap<Integer,BigInteger>> future : futures)
      {
        queryElements.putAll(future.get(1, TimeUnit.DAYS));
      }
    } catch (TimeoutException | ExecutionException e)
    {
      throw new PIRException("Exception in encryption threads.", e);
    }

    es.shutdown();

    return queryElements;
  }
}
