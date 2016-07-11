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
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.pirk.encryption.ModPowAbstraction;
import org.apache.pirk.utils.LogUtils;

/**
 * Runnable class for modular exponential table creation
 * 
 */
public class ExpTableRunnable implements Runnable
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  int dataPartitionBitSize = 0;
  BigInteger NSquared = null;
  ArrayList<BigInteger> queryElements = null;

  // lookup table for exponentiation of query vectors -
  // based on dataPartitionBitSize
  // element -> <power, element^power mod N^2>
  HashMap<BigInteger,HashMap<Integer,BigInteger>> expTable = null;

  public ExpTableRunnable(int dataPartitionBitSizeInput, BigInteger NSquaredInput, ArrayList<BigInteger> queryElementsInput)
  {
    dataPartitionBitSize = dataPartitionBitSizeInput;
    NSquared = NSquaredInput;
    queryElements = queryElementsInput;

    expTable = new HashMap<BigInteger,HashMap<Integer,BigInteger>>();
  }

  @Override
  public void run()
  {
    int maxValue = (int) Math.pow(2, dataPartitionBitSize) - 1;
    for (BigInteger element : queryElements)
    {
      logger.debug("element = " + element.toString(2) + " maxValue = " + maxValue + " dataPartitionBitSize = " + dataPartitionBitSize);

      HashMap<Integer,BigInteger> powMap = new HashMap<Integer,BigInteger>(); // <power, element^power mod N^2>
      for (int i = 0; i <= maxValue; ++i)
      {
        BigInteger value = ModPowAbstraction.modPow(element, BigInteger.valueOf(i), NSquared);

        powMap.put(i, value);
      }
      expTable.put(element, powMap);
    }
    logger.debug("expTable.size() = " + expTable.keySet().size() + " NSquared = " + NSquared.intValue() + " = " + NSquared.toString());
  }

  public HashMap<BigInteger,HashMap<Integer,BigInteger>> getExpTable()
  {
    return expTable;
  }
}
