/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/

package org.apache.pirk.responder.wideskies.storm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.query.wideskies.QueryUtils;
import org.apache.pirk.utils.LogUtils;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;

/**
 * Bolt to extract the partitions of the data record and output {@code <hash(selector), dataPartitions>}
 * <p>
 * Currently receives a {@code <hash(selector), JSON data record>} as input.
 * <p>
 *
 */
public class PartitionDataBolt extends BaseBasicBolt
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  private static final long serialVersionUID = 1L;

  private QueryInfo queryInfo;
  private String queryType;
  private boolean embedSelector;

  private boolean splitPartitions;

  private JSONObject json;
  private ArrayList<BigInteger> partitions;

  @Override
  public void prepare(Map map, TopologyContext context)
  {
    try
    {
      StormUtils.initializeSchemas(map);
    } catch (Exception e)
    {
      logger.error("Unable to initialize schemas in HashBolt. ", e);
    }
    queryInfo = new QueryInfo((Map) map.get(StormConstants.QUERY_INFO_KEY));
    queryType = queryInfo.getQueryType();
    embedSelector = queryInfo.getEmbedSelector();

    json = new JSONObject();
    splitPartitions = (boolean) map.get(StormConstants.SPLIT_PARTITIONS_KEY);

    logger.info("Initialized ExtractAndPartitionDataBolt.");
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector outputCollector)
  {
    int hash = tuple.getIntegerByField(StormConstants.HASH_FIELD);
    json = (JSONObject) tuple.getValueByField(StormConstants.JSON_DATA_FIELD);

    try
    {
      partitions = QueryUtils.partitionDataElement(queryType, json, embedSelector);

      logger.debug("HashSelectorsAndPartitionDataBolt processing " + json.toString() + " outputting results - " + partitions.size());

      // splitPartitions determines whether each partition piece is sent individually or the full Array is sent together.
      // Since processing in the follow-on bolt (EncRowCalcBolt) is computationally expensive, current working theory is
      // that splitting them up allows for better throughput. Though maybe with better knowledge/tuning of Storm internals
      // and paramters (e.g. certain buffer sizes), it may make no difference.
      if (splitPartitions)
      {
        for (BigInteger partition : partitions)
        {
          outputCollector.emit(new Values(hash, partition));
        }
      }
      else
      {
        outputCollector.emit(new Values(hash, partitions));
      }

    } catch (Exception e)
    {
      logger.warn("Failed to partition data for record -- " + json + "\n", e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields(StormConstants.HASH_FIELD, StormConstants.PARTIONED_DATA_FIELD));
  }
}
