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
package org.apache.pirk.querier.wideskies.encrypt;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.utils.LogUtils;
import org.apache.pirk.utils.PIRException;

/**
 * Runnable class for multithreaded PIR encryption
 *
 */
public class EncryptQueryRunnable implements Runnable
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  int dataPartitionBitSize = 0;
  int hashBitSize = 0;
  int start = 0; // start of computing range for the runnable
  int stop = 0; // stop, inclusive, of the computing range for the runnable

  Paillier paillier = null;
  HashMap<Integer,Integer> selectorQueryVecMapping = null;

  TreeMap<Integer,BigInteger> encryptedValues = null; // holds the ordered encrypted values to pull after thread computation is complete

  public EncryptQueryRunnable(int dataPartitionBitSizeInput, int hashBitSizeInput, Paillier paillierInput,
      HashMap<Integer,Integer> selectorQueryVecMappingInput, int startInput, int stopInput)
  {
    dataPartitionBitSize = dataPartitionBitSizeInput;
    hashBitSize = hashBitSizeInput;

    paillier = paillierInput;
    selectorQueryVecMapping = selectorQueryVecMappingInput;

    start = startInput;
    stop = stopInput;

    encryptedValues = new TreeMap<Integer,BigInteger>();
  }

  /**
   * Method to get this runnables encrypted values
   * <p>
   * To be called once the thread computation is complete
   */
  public TreeMap<Integer,BigInteger> getEncryptedValues()
  {
    return encryptedValues;
  }

  @Override
  public void run()
  {
    int i = start;
    while (i <= stop)
    {
      if (selectorQueryVecMapping.containsKey(i))
      {
        int selectorNum = selectorQueryVecMapping.get(i);
        BigInteger valToEnc = (BigInteger.valueOf(2)).pow(selectorNum * dataPartitionBitSize);

        BigInteger encVal;
        try
        {
          encVal = paillier.encrypt(valToEnc);
        } catch (PIRException e)
        {
          e.printStackTrace();
          throw new RuntimeException(e.toString());
        }
        encryptedValues.put(i, encVal);

        logger.debug("selectorNum = " + selectorNum + " valToEnc = " + valToEnc + " envVal = " + encVal);
      }
      else
      {
        BigInteger encZero;
        try
        {
          encZero = paillier.encrypt(BigInteger.ZERO);
        } catch (PIRException e)
        {
          e.printStackTrace();
          throw new RuntimeException(e.toString());
        }
        encryptedValues.put(i, encZero);
      }
      ++i;
    }
  }

}
