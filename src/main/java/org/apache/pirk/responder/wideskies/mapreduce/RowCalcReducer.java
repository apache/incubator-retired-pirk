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
import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.pirk.inputformat.hadoop.BytesArrayWritable;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.responder.wideskies.common.ComputeEncryptedRow;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.LoadDataSchemas;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.utils.FileConst;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Reducer class to calculate the encrypted rows of the encrypted query
 * <p>
 * For each row (as indicated by key = hash(selector)), iterates over each dataElement and calculates the column values.
 * <p>
 * Emits {@code <colNum, colVal>}
 *
 */
public class RowCalcReducer extends Reducer<IntWritable,BytesArrayWritable,LongWritable,Text>
{
  private static final Logger logger = LoggerFactory.getLogger(RowCalcReducer.class);

  private LongWritable keyOut = null;
  private Text valueOut = null;

  private MultipleOutputs<LongWritable,Text> mos = null;

  private FileSystem fs = null;
  private Query query = null;
  private QueryInfo queryInfo = null;

  private boolean useLocalCache = false;
  private boolean limitHitsPerSelector = false;
  private int maxHitsPerSelector = 1000;

  @Override
  public void setup(Context ctx) throws IOException, InterruptedException
  {
    super.setup(ctx);

    keyOut = new LongWritable();
    valueOut = new Text();
    mos = new MultipleOutputs<>(ctx);

    fs = FileSystem.newInstance(ctx.getConfiguration());
    String queryDir = ctx.getConfiguration().get("pirMR.queryInputDir");
    query = Query.readFromHDFSFile(new Path(queryDir), fs);
    queryInfo = query.getQueryInfo();

    try
    {
      SystemConfiguration.setProperty("data.schemas", ctx.getConfiguration().get("data.schemas"));
      SystemConfiguration.setProperty("query.schemas", ctx.getConfiguration().get("query.schemas"));
      SystemConfiguration.setProperty("pir.stopListFile", ctx.getConfiguration().get("pirMR.stopListFile"));

      LoadDataSchemas.initialize(true, fs);
      LoadQuerySchemas.initialize(true, fs);

    } catch (Exception e)
    {
      e.printStackTrace();
    }

    QuerySchema qSchema = LoadQuerySchemas.getSchema(queryInfo.getQueryType());
    DataSchema dSchema = LoadDataSchemas.getSchema(qSchema.getDataSchemaName());

    if (ctx.getConfiguration().get("pirWL.useLocalCache").equals("true"))
    {
      useLocalCache = true;
    }
    if (ctx.getConfiguration().get("pirWL.limitHitsPerSelector").equals("true"))
    {
      limitHitsPerSelector = true;
    }
    maxHitsPerSelector = Integer.parseInt(ctx.getConfiguration().get("pirWL.maxHitsPerSelector"));

    logger.info("RowCalcReducer -- useLocalCache = " + useLocalCache + " limitHitsPerSelector =  " + limitHitsPerSelector + " maxHitsPerSelector = "
        + maxHitsPerSelector);
  }

  @Override
  public void reduce(IntWritable rowIndex, Iterable<BytesArrayWritable> dataElementPartitions, Context ctx) throws IOException, InterruptedException
  {
    logger.debug("Processing reducer for hash = " + rowIndex);
    ctx.getCounter(MRStats.Stats.NUM_HASHES_REDUCER).increment(1);

    if (queryInfo.getUseHDFSExpLookupTable())
    {
      ComputeEncryptedRow.loadCacheFromHDFS(fs, query.getExpFile(rowIndex.get()), query);
    }

    // Compute the encrypted row elements for a query from extracted data partitions
    ArrayList<Tuple2<Long,BigInteger>> encRowValues = ComputeEncryptedRow.computeEncRow(dataElementPartitions, query, rowIndex.get(), limitHitsPerSelector,
        maxHitsPerSelector, useLocalCache);

    // Emit <colNum, colVal>
    for (Tuple2<Long,BigInteger> encRowVal : encRowValues)
    {
      keyOut.set(encRowVal._1);
      BigInteger val = encRowVal._2;
      valueOut.set(val.toString());
      mos.write(FileConst.PIR, keyOut, valueOut);
    }
  }

  @Override
  public void cleanup(Context ctx) throws IOException, InterruptedException
  {
    mos.close();
  }
}
