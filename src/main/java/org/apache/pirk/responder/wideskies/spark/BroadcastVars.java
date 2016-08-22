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

import java.io.Serializable;

import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * Class to hold the broadcast variables
 * 
 */
public class BroadcastVars implements Serializable
{
  private static final long serialVersionUID = 1L;

  private transient JavaSparkContext jsc = null;

  Broadcast<Query> query = null;

  private Broadcast<QueryInfo> queryInfo = null;

  private Broadcast<DataSchema> dataSchema = null;

  private Broadcast<QuerySchema> querySchema = null;

  private Broadcast<String> useLocalCache = null;

  private Broadcast<Boolean> limitHitsPerSelector = null;

  private Broadcast<Integer> maxHitsPerSelector = null;

  private Broadcast<String> expDir = null;

  private Broadcast<String> output = null;

  private Broadcast<Integer> maxBatches = null;

  public BroadcastVars(JavaSparkContext sc)
  {
    jsc = sc;
  }

  public Query getQuery()
  {
    return query.getValue();
  }

  public void setQuery(Query queryIn)
  {
    query = jsc.broadcast(queryIn);
  }

  public QueryInfo getQueryInfo()
  {
    return queryInfo.getValue();
  }

  public void setOutput(String outputIn)
  {
    output = jsc.broadcast(outputIn);
  }

  public String getOutput()
  {
    return output.getValue();
  }

  public void setQueryInfo(QueryInfo queryInfoIn)
  {
    queryInfo = jsc.broadcast(queryInfoIn);
  }

  public void setQuerySchema(QuerySchema qSchemaIn)
  {
    querySchema = jsc.broadcast(qSchemaIn);
  }

  public QuerySchema getQuerySchema()
  {
    return querySchema.getValue();
  }

  public void setDataSchema(DataSchema dSchemaIn)
  {
    dataSchema = jsc.broadcast(dSchemaIn);
  }

  public DataSchema getDataSchema()
  {
    return dataSchema.getValue();
  }

  public void setUseLocalCache(String useLocalCacheInput)
  {
    useLocalCache = jsc.broadcast(useLocalCacheInput);
  }

  public String getUseLocalCache()
  {
    return useLocalCache.getValue();
  }

  public Boolean getLimitHitsPerSelector()
  {
    return limitHitsPerSelector.getValue();
  }

  public void setLimitHitsPerSelector(Boolean limitHitsPerSelectorIn)
  {
    limitHitsPerSelector = jsc.broadcast(limitHitsPerSelectorIn);
  }

  public Integer getMaxHitsPerSelector()
  {
    return maxHitsPerSelector.getValue();
  }

  public void setMaxHitsPerSelector(Integer maxHitsPerSelectorIn)
  {
    maxHitsPerSelector = jsc.broadcast(maxHitsPerSelectorIn);
  }

  public void setExpDir(String expDirIn)
  {
    expDir = jsc.broadcast(expDirIn);
  }

  public String getExpDir()
  {
    return expDir.getValue();
  }

  public Integer getMaxBatches()
  {
    return maxBatches.getValue();
  }

  public void setMaxBatches(Integer maxBatchesIn)
  {
    maxBatches = jsc.broadcast(maxBatchesIn);
  }
}
