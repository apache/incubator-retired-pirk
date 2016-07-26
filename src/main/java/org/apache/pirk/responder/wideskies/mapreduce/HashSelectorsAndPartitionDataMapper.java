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
package org.apache.pirk.responder.wideskies.mapreduce;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pirk.inputformat.hadoop.BytesArrayWritable;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.responder.wideskies.common.HashSelectorAndPartitionData;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.filter.DataFilter;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.apache.pirk.utils.StringUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Initialization mapper for PIR
 * <p>
 * Reads in data, extracts the selector by queryType from each dataElement, performs a keyed hash of the selector, extracts the partitions of the dataElement,
 * and emits {@link <hash(selector), dataPartitions>}
 *
 */
public class HashSelectorsAndPartitionDataMapper extends Mapper<Text,MapWritable,IntWritable,BytesArrayWritable>
{
  private static final Logger logger = LoggerFactory.getLogger(HashSelectorsAndPartitionDataMapper.class);

  private IntWritable keyOut = null;

  HashSet<String> stopList = null;

  private Query query = null;
  private QueryInfo queryInfo = null;
  private QuerySchema qSchema = null;
  private DataSchema dSchema = null;
  private Object filter = null;

  @Override
  public void setup(Context ctx) throws IOException, InterruptedException
  {
    super.setup(ctx);

    logger.info("Setting up the mapper");

    keyOut = new IntWritable();

    FileSystem fs = FileSystem.newInstance(ctx.getConfiguration());

    // Can make this so that it reads multiple queries at one time...
    String queryDir = ctx.getConfiguration().get("pirMR.queryInputDir");
    query = new HadoopFileSystemStore(fs).recall(queryDir, Query.class);
    queryInfo = query.getQueryInfo();

    try
    {
      SystemConfiguration.setProperty("data.schemas", ctx.getConfiguration().get("data.schemas"));
      SystemConfiguration.setProperty("query.schemas", ctx.getConfiguration().get("query.schemas"));
      SystemConfiguration.setProperty("pir.stopListFile", ctx.getConfiguration().get("pirMR.stopListFile"));

      DataSchemaLoader.initialize(true, fs);
      LoadQuerySchemas.initialize(true, fs);

    } catch (Exception e)
    {
      e.printStackTrace();
    }

    if (ctx.getConfiguration().get("pir.allowAdHocQuerySchemas", "false").equals("true"))
    {
      qSchema = queryInfo.getQuerySchema();
    }
    if (qSchema == null)
    {
      qSchema = LoadQuerySchemas.getSchema(queryInfo.getQueryType());
    }
    dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());

    try
    {
      filter = qSchema.getFilterInstance();
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /**
   * The key is the docID/line number and the value is the doc
   */
  @Override
  public void map(Text key, MapWritable value, Context ctx) throws IOException, InterruptedException
  {
    logger.debug("key = " + key.toString());
    logger.debug("value: " + StringUtils.mapWritableToString(value));

    boolean passFilter = true;
    if (filter != null)
    {
      passFilter = ((DataFilter) filter).filterDataElement(value, dSchema);
    }

    if (passFilter)
    {
      // Extract the selector, compute the hash, and partition the data element according to query type
      Tuple2<Integer,BytesArrayWritable> returnTuple = null;
      try
      {
        returnTuple = HashSelectorAndPartitionData.hashSelectorAndFormPartitions(value, qSchema, dSchema, queryInfo);
      } catch (Exception e)
      {
        logger.error("Error in partitioning data element value = " + StringUtils.mapWritableToString(value));
        e.printStackTrace();
      }

      keyOut.set(returnTuple._1);
      ctx.write(keyOut, returnTuple._2);
    }
  }

  @Override
  public void cleanup(Context ctx) throws IOException, InterruptedException
  {
    logger.info("finished with the map - cleaning up ");
  }
}
