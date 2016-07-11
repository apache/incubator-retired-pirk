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
package org.apache.pirk.responder.wideskies.spark;

import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.pirk.encryption.ModPowAbstraction;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.utils.LogUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * Class to generate the query element modular exponentiations
 * <p>
 * 
 */
public class ExpTableGenerator implements PairFlatMapFunction<Integer,Integer,Tuple2<Integer,BigInteger>>
{
  private static final long serialVersionUID = 1L;

  private static Logger logger = LogUtils.getLoggerForThisClass();

  BroadcastVars bbVars = null;
  Query query = null;
  BigInteger NSquared = null;
  int maxValue = 0;

  public ExpTableGenerator(BroadcastVars bbVarsIn)
  {
    bbVars = bbVarsIn;

    query = bbVars.getQuery();
    NSquared = query.getNSquared();

    int dataPartitionBitSize = query.getQueryInfo().getDataPartitionBitSize();
    maxValue = (int) Math.pow(2, dataPartitionBitSize) - 1;
  }

  @Override
  public Iterable<Tuple2<Integer,Tuple2<Integer,BigInteger>>> call(Integer queryHashKey) throws Exception
  {
    // queryHashKey -> <<power>,<element^power mod N^2>>
    ArrayList<Tuple2<Integer,Tuple2<Integer,BigInteger>>> modExp = new ArrayList<Tuple2<Integer,Tuple2<Integer,BigInteger>>>();

    BigInteger element = query.getQueryElement(queryHashKey);
    for (int i = 0; i <= maxValue; ++i)
    {
      BigInteger modPow = ModPowAbstraction.modPow(element, BigInteger.valueOf(i), NSquared);
      Tuple2<Integer,BigInteger> modPowTuple = new Tuple2<Integer,BigInteger>(i, modPow);
      modExp.add(new Tuple2<Integer,Tuple2<Integer,BigInteger>>(queryHashKey, modPowTuple));
    }

    return modExp;
  }
}
