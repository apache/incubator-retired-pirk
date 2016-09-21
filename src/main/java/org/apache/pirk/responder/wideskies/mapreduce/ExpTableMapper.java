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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pirk.encryption.IntegerMathAbstraction;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to generate the expTable given the input query vectors
 *
 */
public class ExpTableMapper extends Mapper<LongWritable,Text,Text,Text>
{
  private static final Logger logger = LoggerFactory.getLogger(ExpTableMapper.class);

  private Text valueOut = null;

  private int maxValue = 0;
  private BigInteger NSquared = null;
  private Query query = null;

  @Override
  public void setup(Context ctx) throws IOException, InterruptedException
  {
    super.setup(ctx);

    valueOut = new Text();

    String queryDir = ctx.getConfiguration().get("pirMR.queryInputDir");
    query = new HadoopFileSystemStore(FileSystem.newInstance(ctx.getConfiguration())).recall(queryDir, Query.class);

    int dataPartitionBitSize = query.getQueryInfo().getDataPartitionBitSize();
    maxValue = (int) Math.pow(2, dataPartitionBitSize) - 1;

    NSquared = query.getNSquared();
  }

  // key is line number; value is the index of the queryVec
  @Override
  public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException
  {
    logger.info("key = " + key.toString() + " value = " + value.toString());

    BigInteger element = query.getQueryElement(Integer.parseInt(value.toString()));
    for (int i = 0; i <= maxValue; ++i)
    {
      BigInteger modPow = IntegerMathAbstraction.modPow(element, BigInteger.valueOf(i), NSquared);

      valueOut.set(i + "-" + modPow.toString()); // val: <power>-<element^power mod N^2>
      ctx.write(value, valueOut);
    }
  }
}
