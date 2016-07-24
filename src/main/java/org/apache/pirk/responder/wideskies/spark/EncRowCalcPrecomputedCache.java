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
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.responder.wideskies.common.ComputeEncryptedRow;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.LoadDataSchemas;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.utils.LogUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * Functionality for computing the encrypted rows using a pre-computed, passed in modular exponentiation lookup table
 */
public class EncRowCalcPrecomputedCache implements
    PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Tuple2<Integer,BigInteger>>,Iterable<ArrayList<BigInteger>>>>,Long,BigInteger>
{
  private static final long serialVersionUID = 1L;

  private static Logger logger = LogUtils.getLoggerForThisClass();

  Accumulators accum = null;
  BroadcastVars bVars = null;

  Query query = null;
  QueryInfo queryInfo = null;
  QuerySchema qSchema = null;
  DataSchema dSchema = null;

  boolean useLocalCache = false;
  boolean limitHitsPerSelector = false;
  int maxHitsPerSelector = 0;
  HashMap<Integer,BigInteger> expTable = null;

  public EncRowCalcPrecomputedCache(Accumulators accumIn, BroadcastVars bvIn)
  {
    accum = accumIn;
    bVars = bvIn;

    query = bVars.getQuery();
    queryInfo = bVars.getQueryInfo();
    qSchema = bVars.getQuerySchema();
    dSchema = bVars.getDataSchema();

    if (bVars.getUseLocalCache().equals("true"))
    {
      useLocalCache = true;
    }
    limitHitsPerSelector = bVars.getLimitHitsPerSelector();
    maxHitsPerSelector = bVars.getMaxHitsPerSelector();

    expTable = new HashMap<Integer,BigInteger>();

    logger.info("Initialized EncRowCalcPrecomputedCache - limitHitsPerSelector = " + limitHitsPerSelector + " maxHitsPerSelector = " + maxHitsPerSelector);
  }

  @Override
  public Iterable<Tuple2<Long,BigInteger>> call(Tuple2<Integer,Tuple2<Iterable<Tuple2<Integer,BigInteger>>,Iterable<ArrayList<BigInteger>>>> hashDocTuple)
      throws Exception
  {
    ArrayList<Tuple2<Long,BigInteger>> returnPairs = new ArrayList<Tuple2<Long,BigInteger>>();

    int rowIndex = hashDocTuple._1;
    accum.incNumHashes(1);

    // expTable: <<power>,<element^power mod N^2>>
    expTable.clear();
    Iterable<Tuple2<Integer,BigInteger>> expTableIterable = hashDocTuple._2._1;
    for (Tuple2<Integer,BigInteger> entry : expTableIterable)
    {
      expTable.put(entry._1, entry._2);
    }

    Iterable<ArrayList<BigInteger>> dataPartitions = hashDocTuple._2._2;

    // logger.debug("Encrypting row = " + rowIndex);
    // long startTime = System.currentTimeMillis();

    // Compute the encrypted row elements for a query from extracted data partitions
    ArrayList<Tuple2<Long,BigInteger>> encRowValues = ComputeEncryptedRow.computeEncRowCacheInput(dataPartitions, expTable, rowIndex, limitHitsPerSelector,
        maxHitsPerSelector);

    // long endTime = System.currentTimeMillis();
    // logger.info("Completed encrypting row = " + rowIndex + " duration = " + (endTime-startTime));

    returnPairs.addAll(encRowValues);

    return returnPairs;
  }
}
