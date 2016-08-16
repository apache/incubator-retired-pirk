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

import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.query.wideskies.QueryUtils;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaRegistry;
import org.apache.pirk.utils.KeyedHash;

import org.apache.storm.Config;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Scheme used by spout to retrieve and hash selector from JSON data on Kafka.
 */
public class PirkHashScheme extends StringScheme implements Scheme
{

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PirkHashScheme.class);

  private QueryInfo queryInfo;

  transient private JSONParser parser;
  transient private JSONObject json;
  private ArrayList<List<Object>> values = new ArrayList<List<Object>>();;
  private List<Object> value = new ArrayList<Object>();
  private boolean initialized = false;
  private QuerySchema qSchema;
  private Config conf;

  public PirkHashScheme(Config conf)
  {
    this.conf = conf;
  }

  public List<Object> deserialize(ByteBuffer bytes)
  {
    if (!initialized)
    {
      parser = new JSONParser();
      queryInfo = new QueryInfo((Map) conf.get(StormConstants.QUERY_INFO_KEY));

      StormUtils.initializeSchemas(conf, "hashScheme");

      if ((boolean) conf.get(StormConstants.ALLOW_ADHOC_QSCHEMAS_KEY))
      {
        qSchema = queryInfo.getQuerySchema();
      }
      if (qSchema == null)
      {
        qSchema = QuerySchemaRegistry.get(queryInfo.getQueryType());
      }

      initialized = true;
    }
    String str = super.deserializeString(bytes);

    try
    {
      json = (JSONObject) parser.parse(str);
    } catch (ParseException e)
    {
      json = null;
      logger.warn("ParseException. ", e);
    }
    String selector = QueryUtils.getSelectorByQueryTypeJSON(qSchema, json);
    int hash = KeyedHash.hash(queryInfo.getHashKey(), queryInfo.getHashBitSize(), selector);

    return new Values(hash, json);
  }

  public Fields getOutputFields()
  {
    return new Fields(StormConstants.HASH_FIELD, StormConstants.JSON_DATA_FIELD);
  }

}
