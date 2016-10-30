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
package org.apache.pirk.responder.wideskies.spark;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.responder.wideskies.common.ComputeEncryptedRow;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Functionality for computing the encrypted rows using a pre-computed, passed in modular exponentiation lookup table
 */
public class EncRowCalcPrecomputedCache
    implements PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Tuple2<Integer,BigInteger>>,Iterable<List<BigInteger>>>>,Long,BigInteger>
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(EncRowCalcPrecomputedCache.class);

  private Accumulators accum = null;

  Query query = null;

  private boolean limitHitsPerSelector = false;
  private int maxHitsPerSelector = 0;

  private HashMap<Integer,BigInteger> expTable = null;

  public EncRowCalcPrecomputedCache(Accumulators accumIn, BroadcastVars bvIn)
  {
    accum = accumIn;

    query = bvIn.getQuery();

    limitHitsPerSelector = bvIn.getLimitHitsPerSelector();
    maxHitsPerSelector = bvIn.getMaxHitsPerSelector();

    expTable = new HashMap<>();

    logger.info("Initialized EncRowCalcPrecomputedCache - limitHitsPerSelector = " + limitHitsPerSelector + " maxHitsPerSelector = " + maxHitsPerSelector);
  }

  @Override
  public Iterator<Tuple2<Long, BigInteger>> call(Tuple2<Integer,Tuple2<Iterable<Tuple2<Integer,BigInteger>>,Iterable<List<BigInteger>>>> hashDocTuple)
      throws Exception
  {
    List<Tuple2<Long,BigInteger>> returnPairs = new ArrayList<>();

    int rowIndex = hashDocTuple._1;
    accum.incNumHashes(1);

    // expTable: <<power>,<element^power mod N^2>>
    expTable.clear();
    Iterable<Tuple2<Integer,BigInteger>> expTableIterable = hashDocTuple._2._1;
    for (Tuple2<Integer,BigInteger> entry : expTableIterable)
    {
      expTable.put(entry._1, entry._2);
    }

    Iterable<List<BigInteger>> dataPartitions = hashDocTuple._2._2;

    // logger.debug("Encrypting row = " + rowIndex);
    // long startTime = System.currentTimeMillis();

    // Compute the encrypted row elements for a query from extracted data partitions
    List<Tuple2<Long,BigInteger>> encRowValues = ComputeEncryptedRow.computeEncRowCacheInput(dataPartitions, expTable, rowIndex, limitHitsPerSelector,
        maxHitsPerSelector);

    // long endTime = System.currentTimeMillis();
    // logger.info("Completed encrypting row = " + rowIndex + " duration = " + (endTime-startTime));

    returnPairs.addAll(encRowValues);

    return returnPairs.iterator();
  }
}
