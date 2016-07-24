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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reducer class to construct the final Response object
 * 
 */
public class FinalResponseReducer extends Reducer<LongWritable,Text,LongWritable,Text>
{
  private static final Logger logger = LoggerFactory.getLogger(FinalResponseReducer.class);

  private MultipleOutputs<LongWritable,Text> mos = null;

  private Response response = null;
  private String outputFile = null;
  private FileSystem fs = null;
  private HadoopFileSystemStore storage = null;
  private QueryInfo queryInfo = null;

  @Override
  public void setup(Context ctx) throws IOException, InterruptedException
  {
    super.setup(ctx);

    Text outputValue = new Text();
    mos = new MultipleOutputs<>(ctx);

    fs = FileSystem.newInstance(ctx.getConfiguration());
    storage = new HadoopFileSystemStore(fs);
    String queryDir = ctx.getConfiguration().get("pirMR.queryInputDir");
    Query query = storage.recall(queryDir, Query.class);
    QueryInfo queryInfo = query.getQueryInfo();

    outputFile = ctx.getConfiguration().get("pirMR.outputFile");

    response = new Response(queryInfo);
  }

  @Override
  public void reduce(LongWritable colNum, Iterable<Text> colVals, Context ctx) throws IOException, InterruptedException
  {
    logger.debug("Processing reducer for colNum = " + colNum.toString());
    ctx.getCounter(MRStats.Stats.NUM_COLUMNS).increment(1);

    BigInteger column = null;
    for (Text val : colVals) // there is only one column value
    {
      column = new BigInteger(val.toString());
      logger.debug("colNum = " + (int) colNum.get() + " column = " + column.toString());
    }
    response.addElement((int) colNum.get(), column);
  }

  @Override
  public void cleanup(Context ctx) throws IOException, InterruptedException
  {
    storage.store(outputFile, response);
    mos.close();
  }
}
