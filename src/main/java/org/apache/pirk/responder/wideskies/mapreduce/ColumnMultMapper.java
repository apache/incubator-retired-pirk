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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.pirk.utils.CSVOutputUtils;
import org.apache.pirk.utils.LogUtils;

/**
 * Pass through mapper for encrypted column multiplication
 *
 */
public class ColumnMultMapper extends Mapper<LongWritable,Text,LongWritable,Text>
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  LongWritable keyOut = null;
  Text valueOut = null;

  @Override
  public void setup(Context ctx) throws IOException, InterruptedException
  {
    super.setup(ctx);

    keyOut = new LongWritable();
    valueOut = new Text();
  }

  @Override
  public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException
  {
    logger.debug("key = " + key.toString() + " value = " + value.toString());

    String tokens[] = CSVOutputUtils.extractCSVOutput(value);
    logger.debug("value = " + value.toString() + " tokens[0] = " + tokens[0] + " tokens[1] = " + tokens[1]);

    keyOut.set(Integer.parseInt(tokens[0])); // colNum
    valueOut.set(tokens[1]); // colValue
    ctx.write(keyOut, valueOut);
  }
}
