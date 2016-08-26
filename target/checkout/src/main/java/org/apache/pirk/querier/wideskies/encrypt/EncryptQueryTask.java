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

import java.math.BigInteger;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.utils.PIRException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable class for multithreaded PIR encryption
 */
class EncryptQueryTask implements Callable<SortedMap<Integer,BigInteger>>
{
  private static final Logger logger = LoggerFactory.getLogger(EncryptQueryTask.class);

  private final int dataPartitionBitSize;
  private final int start; // start of computing range for the runnable
  private final int stop; // stop, inclusive, of the computing range for the runnable

  private final Paillier paillier;
  private final Map<Integer,Integer> selectorQueryVecMapping;

  public EncryptQueryTask(int dataPartitionBitSizeInput, Paillier paillierInput, Map<Integer,Integer> selectorQueryVecMappingInput, int startInput,
      int stopInput)
  {
    dataPartitionBitSize = dataPartitionBitSizeInput;

    paillier = paillierInput;
    selectorQueryVecMapping = selectorQueryVecMappingInput;

    start = startInput;
    stop = stopInput;
  }

  @Override
  public SortedMap<Integer,BigInteger> call() throws PIRException
  {
    // holds the ordered encrypted values to pull after thread computation is complete
    SortedMap<Integer,BigInteger> encryptedValues = new TreeMap<>();
    for (int i = start; i <= stop; i++)
    {
      Integer selectorNum = selectorQueryVecMapping.get(i);
      BigInteger valToEnc = (selectorNum == null) ? BigInteger.ZERO : (BigInteger.valueOf(2)).pow(selectorNum * dataPartitionBitSize);
      BigInteger encVal = paillier.encrypt(valToEnc);
      encryptedValues.put(i, encVal);
      logger.debug("selectorNum = " + selectorNum + " valToEnc = " + valToEnc + " envVal = " + encVal);
    }

    return encryptedValues;
  }
}
