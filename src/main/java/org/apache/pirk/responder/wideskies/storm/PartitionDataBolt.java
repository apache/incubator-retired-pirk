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
import java.util.List;
import java.util.Map;

import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.query.wideskies.QueryUtils;

import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaRegistry;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.slf4j.LoggerFactory;

/**
 * Bolt to extract the partitions of the data record and output {@code <hash(selector), dataPartitions>}
 * <p>
 * Currently receives a {@code <hash(selector), JSON data record>} as input.
 * <p>
 *
 */
public class PartitionDataBolt extends BaseBasicBolt
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PartitionDataBolt.class);

  private static final long serialVersionUID = 1L;

  private QueryInfo queryInfo;
  private String queryType;
  private QuerySchema qSchema = null;

  private boolean embedSelector;

  private boolean splitPartitions;

  private JSONObject json;
  private List<BigInteger> partitions;

  @Override
  public void prepare(Map map, TopologyContext context)
  {
    queryInfo = new QueryInfo((Map) map.get(StormConstants.QUERY_INFO_KEY));
    queryType = queryInfo.getQueryType();
    embedSelector = queryInfo.getEmbedSelector();
    logger.info("partition databolt hdfs = " + map.get(StormConstants.USE_HDFS));
    StormUtils.initializeSchemas(map, "partition");
    try
    {
      if ((boolean) map.get(StormConstants.ALLOW_ADHOC_QSCHEMAS_KEY))
      {
        qSchema = queryInfo.getQuerySchema();
      }
      if (qSchema == null)
      {
        qSchema = QuerySchemaRegistry.get(queryType);
      }
    } catch (Exception e)
    {
      logger.error("Unable to initialize schemas in PartitionDataBolt. ", e);
    }

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
      partitions = QueryUtils.partitionDataElement(qSchema, json, embedSelector);

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
