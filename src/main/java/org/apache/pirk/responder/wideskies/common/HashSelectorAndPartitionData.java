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
package org.apache.pirk.responder.wideskies.common;

import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;
import org.apache.pirk.inputformat.hadoop.BytesArrayWritable;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.query.wideskies.QueryUtils;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.utils.KeyedHash;
import org.apache.pirk.utils.LogUtils;
import org.json.simple.JSONObject;

import scala.Tuple2;

/**
 * Given a MapWritable dataElement, this class gives the common functionality to extract the selector by queryType from each dataElement, perform a keyed hash
 * of the selector, extract the partitions of the dataElement, and outputs {@code <hash(selector), dataPartitions>}
 */
public class HashSelectorAndPartitionData
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  public static Tuple2<Integer,ArrayList<BigInteger>> hashSelectorAndFormPartitionsBigInteger(MapWritable dataElement, QuerySchema qSchema, DataSchema dSchema,
      QueryInfo queryInfo) throws Exception
  {
    Tuple2<Integer,ArrayList<BigInteger>> returnTuple = null;

    // Pull the selector based on the query type
    String selector = QueryUtils.getSelectorByQueryType(dataElement, qSchema, dSchema);
    int hash = KeyedHash.hash(queryInfo.getHashKey(), queryInfo.getHashBitSize(), selector);
    logger.debug("selector = " + selector + " hash = " + hash);

    // Extract the data bits based on the query type
    // Partition by the given partitionSize
    ArrayList<BigInteger> hitValPartitions = QueryUtils.partitionDataElement(dataElement, qSchema, dSchema, queryInfo.getEmbedSelector());

    returnTuple = new Tuple2<Integer,ArrayList<BigInteger>>(hash, hitValPartitions);

    return returnTuple;
  }

  public static Tuple2<Integer,BytesArrayWritable> hashSelectorAndFormPartitions(MapWritable dataElement, QuerySchema qSchema, DataSchema dSchema,
      QueryInfo queryInfo) throws Exception
  {
    Tuple2<Integer,BytesArrayWritable> returnTuple = null;

    // Pull the selector based on the query type
    String selector = QueryUtils.getSelectorByQueryType(dataElement, qSchema, dSchema);
    int hash = KeyedHash.hash(queryInfo.getHashKey(), queryInfo.getHashBitSize(), selector);
    logger.debug("selector = " + selector + " hash = " + hash);

    // Extract the data bits based on the query type
    // Partition by the given partitionSize
    ArrayList<BigInteger> hitValPartitions = QueryUtils.partitionDataElement(dataElement, qSchema, dSchema, queryInfo.getEmbedSelector());
    BytesArrayWritable bAW = new BytesArrayWritable(hitValPartitions);

    returnTuple = new Tuple2<Integer,BytesArrayWritable>(hash, bAW);

    return returnTuple;
  }

  public static Tuple2<Integer,ArrayList<BigInteger>> hashSelectorAndFormPartitions(JSONObject json, QueryInfo queryInfo) throws Exception
  {
    Tuple2<Integer,ArrayList<BigInteger>> returnTuple = null;

    // Pull the selector based on the query type
    String selector = QueryUtils.getSelectorByQueryTypeJSON(queryInfo.getQueryType(), json);
    int hash = KeyedHash.hash(queryInfo.getHashKey(), queryInfo.getHashBitSize(), selector);
    logger.debug("selector = " + selector + " hash = " + hash);

    // Extract the data bits based on the query type
    // Partition by the given partitionSize
    ArrayList<BigInteger> hitValPartitions = QueryUtils.partitionDataElement(queryInfo.getQueryType(), json, queryInfo.getEmbedSelector());

    returnTuple = new Tuple2<Integer,ArrayList<BigInteger>>(hash, hitValPartitions);

    return returnTuple;
  }
}
