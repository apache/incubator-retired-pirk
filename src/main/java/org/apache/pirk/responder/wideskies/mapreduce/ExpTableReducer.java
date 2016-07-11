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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.apache.pirk.utils.FileConst;
import org.apache.pirk.utils.LogUtils;

/**
 * Reducer class to complete the exp lookup table and add to the Query object
 *
 */
public class ExpTableReducer extends Reducer<Text,Text,Text,Text>
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  private MultipleOutputs<Text,Text> mos = null;
  String reducerID = null;

  @Override
  public void setup(Context ctx) throws IOException, InterruptedException
  {
    super.setup(ctx);
    mos = new MultipleOutputs<Text,Text>(ctx);
    reducerID = String.format("%05d", ctx.getTaskAttemptID().getTaskID().getId());
    logger.info("reducerID = " + reducerID);
  }

  @Override
  public void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException
  {
    logger.debug("Processing reducer for key = " + key.toString());

    for (Text val : vals) // val: <power>-<element^power mod N^2>
    {
      mos.write(FileConst.EXP, key, val);
    }
    mos.write(FileConst.PIR, key, reducerID);
  }

  @Override
  public void cleanup(Context ctx) throws IOException, InterruptedException
  {
    mos.close();
  }
}
