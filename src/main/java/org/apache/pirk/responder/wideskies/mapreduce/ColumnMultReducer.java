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
package org.apache.pirk.responder.wideskies.mapreduce;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.utils.FileConst;
import org.apache.pirk.utils.LogUtils;

/**
 * Reducer to perform encrypted column multiplication
 * 
 */
public class ColumnMultReducer extends Reducer<LongWritable,Text,LongWritable,Text>
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  Text outputValue = null;
  private MultipleOutputs<LongWritable,Text> mos = null;

  Query query = null;

  @Override
  public void setup(Context ctx) throws IOException, InterruptedException
  {
    super.setup(ctx);

    outputValue = new Text();
    mos = new MultipleOutputs<LongWritable,Text>(ctx);

    FileSystem fs = FileSystem.newInstance(ctx.getConfiguration());
    String queryDir = ctx.getConfiguration().get("pirMR.queryInputDir");
    query = Query.readFromHDFSFile(new Path(queryDir), fs);
  }

  @Override
  public void reduce(LongWritable colNum, Iterable<Text> colVals, Context ctx) throws IOException, InterruptedException
  {
    logger.debug("Processing reducer for colNum = " + colNum.toString());
    ctx.getCounter(MRStats.Stats.NUM_COLUMNS).increment(1);

    BigInteger column = BigInteger.valueOf(1);
    for (Text val : colVals)
    {
      BigInteger valBI = new BigInteger(val.toString());
      column = (column.multiply(valBI)).mod(query.getNSquared());
      logger.debug("valBI = " + valBI.toString() + " column = " + column.toString());
    }
    logger.debug("final column value = " + column.toString());
    outputValue.set(column.toString());
    mos.write(FileConst.PIR_COLS, colNum, outputValue);
  }

  @Override
  public void cleanup(Context ctx) throws IOException, InterruptedException
  {
    mos.close();
  }
}
