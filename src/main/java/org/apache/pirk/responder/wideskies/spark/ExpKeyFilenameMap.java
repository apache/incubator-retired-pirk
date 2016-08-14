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

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Class to map the query hash to its modular exponentiation lookup file in hdfs
 */
public class ExpKeyFilenameMap implements PairFlatMapFunction<Iterator<Tuple2<Integer,Iterable<Tuple2<Integer,BigInteger>>>>,Integer,String>
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(ExpKeyFilenameMap.class);

  private String expOutDir = null;

  public ExpKeyFilenameMap(BroadcastVars bbVarsIn)
  {
    expOutDir = bbVarsIn.getExpDir();
  }

  @Override
  public Iterable<Tuple2<Integer,String>> call(Iterator<Tuple2<Integer,Iterable<Tuple2<Integer,BigInteger>>>> iter) throws Exception
  {
    ArrayList<Tuple2<Integer,String>> keyFileList = new ArrayList<>();

    FileSystem fs = FileSystem.get(new Configuration());

    // Form the filename for the exp table portion that corresponds to this partition
    int taskId = TaskContext.getPartitionId();
    logger.info("taskId = " + taskId);

    String fileName = expOutDir + "/exp-" + String.format("%05d", taskId);
    logger.info("fileName = " + fileName);

    // Iterate over the elements of the partition
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(fileName), true)));
    while (iter.hasNext())
    {
      // <queryHash, <<power>,<element^power mod N^2>>
      Tuple2<Integer,Iterable<Tuple2<Integer,BigInteger>>> expTuple = iter.next();
      int queryHash = expTuple._1;

      // Record the queryHash -> fileName
      keyFileList.add(new Tuple2<>(queryHash, fileName));

      // Write the partition elements to the corresponding exp table file
      // each line: queryHash,<power>-<element^power mod N^2>
      for (Tuple2<Integer,BigInteger> modPow : expTuple._2)
      {
        String lineOut = queryHash + "," + modPow._1 + "-" + modPow._2;
        bw.write(lineOut);
        bw.newLine();
      }
    }
    bw.close();

    return keyFileList;
  }
}
