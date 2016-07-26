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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.responder.wideskies.common.ComputeEncryptedRow;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Function to calculate the encrypted rows of the encrypted query
 * <p>
 * For each row (as indicated by key = hash(selector)), iterates over each dataElement and calculates the column values.
 * <p>
 * Emits {@code <colNum, colVal>}
 *
 */
public class EncRowCalc implements PairFlatMapFunction<Tuple2<Integer,Iterable<ArrayList<BigInteger>>>,Long,BigInteger>
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(EncRowCalc.class);

  private Accumulators accum = null;
  private BroadcastVars bVars = null;

  private Query query = null;
  private QueryInfo queryInfo = null;

  private boolean useLocalCache = false;
  private boolean limitHitsPerSelector = false;
  private int maxHitsPerSelector = 0;

  public EncRowCalc(Accumulators accumIn, BroadcastVars bvIn)
  {
    accum = accumIn;
    bVars = bvIn;

    query = bVars.getQuery();
    queryInfo = bVars.getQueryInfo();
    if (bVars.getUseLocalCache().equals("true"))
    {
      useLocalCache = true;
    }
    limitHitsPerSelector = bVars.getLimitHitsPerSelector();
    maxHitsPerSelector = bVars.getMaxHitsPerSelector();

    logger.info("Initialized EncRowCalc - limitHitsPerSelector = " + limitHitsPerSelector + " maxHitsPerSelector = " + maxHitsPerSelector);
  }

  @Override
  public Iterable<Tuple2<Long,BigInteger>> call(Tuple2<Integer,Iterable<ArrayList<BigInteger>>> hashDocTuple) throws Exception
  {
    ArrayList<Tuple2<Long,BigInteger>> returnPairs = new ArrayList<>();

    int rowIndex = hashDocTuple._1;
    accum.incNumHashes(1);

    if (queryInfo.getUseHDFSExpLookupTable())
    {
      FileSystem fs = null;
      try
      {
        fs = FileSystem.get(new Configuration());
      } catch (IOException e)
      {
        e.printStackTrace();
      }
      ComputeEncryptedRow.loadCacheFromHDFS(fs, query.getExpFile(rowIndex), query);
    }

    // logger.debug("Encrypting row = " + rowIndex);
    // long startTime = System.currentTimeMillis();

    // Compute the encrypted row elements for a query from extracted data partitions
    ArrayList<Tuple2<Long,BigInteger>> encRowValues = ComputeEncryptedRow.computeEncRowBI(hashDocTuple._2, query, rowIndex, limitHitsPerSelector,
        maxHitsPerSelector, useLocalCache);

    // long endTime = System.currentTimeMillis();
    // logger.debug("Completed encrypting row = " + rowIndex + " duration = " + (endTime-startTime));

    returnPairs.addAll(encRowValues);

    return returnPairs;
  }
}
