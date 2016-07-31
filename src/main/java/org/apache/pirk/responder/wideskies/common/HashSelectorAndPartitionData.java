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
package org.apache.pirk.responder.wideskies.common;

import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.hadoop.io.MapWritable;
import org.apache.pirk.inputformat.hadoop.BytesArrayWritable;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.query.wideskies.QueryUtils;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.utils.KeyedHash;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Given a MapWritable or JSON formatted dataElement, this class gives the common functionality to extract the selector by queryType from each dataElement,
 * perform a keyed hash of the selector, extract the partitions of the dataElement, and outputs {@code <hash(selector), dataPartitions>}
 */
public class HashSelectorAndPartitionData
{
  private static final Logger logger = LoggerFactory.getLogger(HashSelectorAndPartitionData.class);

  public static Tuple2<Integer,ArrayList<BigInteger>> hashSelectorAndFormPartitionsBigInteger(MapWritable dataElement, QuerySchema qSchema, DataSchema dSchema,
      QueryInfo queryInfo) throws Exception
  {
    Tuple2<Integer,ArrayList<BigInteger>> returnTuple;

    // Pull the selector based on the query type
    String selector = QueryUtils.getSelectorByQueryType(dataElement, qSchema, dSchema);
    int hash = KeyedHash.hash(queryInfo.getHashKey(), queryInfo.getHashBitSize(), selector);
    logger.debug("selector = " + selector + " hash = " + hash);

    // Extract the data bits based on the query type
    // Partition by the given partitionSize
    ArrayList<BigInteger> hitValPartitions = QueryUtils.partitionDataElement(dataElement, qSchema, dSchema, queryInfo.getEmbedSelector());

    returnTuple = new Tuple2<>(hash, hitValPartitions);

    return returnTuple;
  }

  public static Tuple2<Integer,BytesArrayWritable> hashSelectorAndFormPartitions(MapWritable dataElement, QuerySchema qSchema, DataSchema dSchema,
      QueryInfo queryInfo) throws Exception
  {
    Tuple2<Integer,BytesArrayWritable> returnTuple;

    // Pull the selector based on the query type
    String selector = QueryUtils.getSelectorByQueryType(dataElement, qSchema, dSchema);
    int hash = KeyedHash.hash(queryInfo.getHashKey(), queryInfo.getHashBitSize(), selector);
    logger.debug("selector = " + selector + " hash = " + hash);

    // Extract the data bits based on the query type
    // Partition by the given partitionSize
    ArrayList<BigInteger> hitValPartitions = QueryUtils.partitionDataElement(dataElement, qSchema, dSchema, queryInfo.getEmbedSelector());
    BytesArrayWritable bAW = new BytesArrayWritable(hitValPartitions);

    returnTuple = new Tuple2<>(hash, bAW);

    return returnTuple;
  }

  public static Tuple2<Integer,ArrayList<BigInteger>> hashSelectorAndFormPartitions(JSONObject json, QueryInfo queryInfo, QuerySchema qSchema) throws Exception
  {
    Tuple2<Integer,ArrayList<BigInteger>> returnTuple;

    // Pull the selector based on the query type
    String selector = QueryUtils.getSelectorByQueryTypeJSON(qSchema, json);
    int hash = KeyedHash.hash(queryInfo.getHashKey(), queryInfo.getHashBitSize(), selector);
    logger.debug("selector = " + selector + " hash = " + hash);

    // Extract the data bits based on the query type
    // Partition by the given partitionSize
    ArrayList<BigInteger> hitValPartitions = QueryUtils.partitionDataElement(qSchema, json, queryInfo.getEmbedSelector());

    returnTuple = new Tuple2<>(hash, hitValPartitions);

    return returnTuple;
  }
}
