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

import org.apache.hadoop.io.MapWritable;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.responder.wideskies.common.HashSelectorAndPartitionData;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Function to extract the selector by queryType from each dataElement, perform a keyed hash of the selector, extract the partitions of the dataElement, and
 * output {@code <hash(selector), dataPartitions>}
 *
 */
public class HashSelectorsAndPartitionData implements PairFunction<MapWritable,Integer,ArrayList<BigInteger>>
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(HashSelectorsAndPartitionData.class);

  Accumulators accum = null;
  BroadcastVars bVars = null;

  private QueryInfo queryInfo = null;
  private QuerySchema qSchema = null;
  private DataSchema dSchema = null;

  public HashSelectorsAndPartitionData(Accumulators accumIn, BroadcastVars bvIn)
  {
    accum = accumIn;
    bVars = bvIn;

    queryInfo = bVars.getQueryInfo();
    qSchema = bVars.getQuerySchema();
    dSchema = bVars.getDataSchema();

    logger.info("Initialized HashSelectorsAndPartitionData");
  }

  @Override
  public Tuple2<Integer,ArrayList<BigInteger>> call(MapWritable doc) throws Exception
  {
    Tuple2<Integer,ArrayList<BigInteger>> returnTuple;

    // Extract the selector, compute the hash, and partition the data element according to query type
    returnTuple = HashSelectorAndPartitionData.hashSelectorAndFormPartitionsBigInteger(doc, qSchema, dSchema, queryInfo);

    return returnTuple;
  }
}
