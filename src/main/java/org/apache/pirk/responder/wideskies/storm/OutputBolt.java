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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.utils.LogUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class OutputBolt extends BaseRichBolt
{
  private OutputCollector outputCollector;
  private QueryInfo queryInfo;
  private Response response;
  private String outputFile;
  private boolean hdfs;
  private String hdfsUri;
  private Integer flushCounter = 0;
  private ArrayList<Tuple> tuplesToAck = new ArrayList<Tuple>();
  private Integer totalFlushSigs;

  // This latch just serves as a hook for testing.
  public static CountDownLatch latch = new CountDownLatch(1);

  // This is the main object here.  It holds column Id -> product
  private HashMap<Long,BigInteger> resultsMap = new HashMap<Long,BigInteger>();

  BigInteger colVal;
  BigInteger colVal2;
  BigInteger colMult;

  private BigInteger nSquared;

  private static Logger logger = LogUtils.getLoggerForThisClass();

  @Override public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector)
  {
    outputCollector = collector;

    totalFlushSigs = ((Long) map.get(StormConstants.ENCCOLMULTBOLT_PARALLELISM_KEY)).intValue();
    outputFile = (String) map.get(StormConstants.OUTPUT_FILE_KEY);
    hdfs = (boolean) map.get(StormConstants.USE_HDFS);
    if (hdfs)
      hdfsUri = (String) map.get(StormConstants.HDFS_URI_KEY);
    nSquared = new BigInteger((String) map.get(StormConstants.N_SQUARED_KEY));
    queryInfo = new QueryInfo((Map) map.get(StormConstants.QUERY_INFO_KEY));
    response = new Response(queryInfo);
    logger.info("Intitialized OutputBolt.");
  }

  @Override public void execute(Tuple tuple)
  {

    // Receives (colIndex, colProduct) pairs and writes them to HDFS whenever enough flush signals have been received
    // or a timeout.

    Long colIndex = tuple.getLongByField(StormConstants.COLUMN_INDEX_ECM_FIELD);
    colVal = (BigInteger) tuple.getValueByField(StormConstants.COLUMN_PRODUCT_FIELD);

    if (colIndex == -1)
    {
      flushCounter++;

      logger.debug("Received " + flushCounter + " output flush signals out of " + totalFlushSigs);

      if (flushCounter == totalFlushSigs)
      {
        logger.info("TimeToFlush reached - outputting response file with columns.size = " + resultsMap.keySet().size());
        try
        {
          String timestamp = (new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date())).toString();
          for (long cv : resultsMap.keySet())
          {
            response.addElement((int) cv, resultsMap.get(cv));
          }
          if (hdfs)
          {
            FileSystem fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
            response.writeToHDFSFile(new Path(outputFile + "_" + timestamp), fs);
          }
          else
          {   // In order to accomodate testing, this does not currently include timestamp.
            // Should probably be fixed, but this will not likely be used outside of testing.
            response.writeToFile(new File(outputFile));
            for (long cv : resultsMap.keySet())
            {
              response.addElement((int) cv, resultsMap.get(cv));
              logger.debug("column = " + cv + ", value = " + resultsMap.get(cv).toString());
            }
          }

        } catch (IOException e)
        {
          logger.warn("Unable to write output file.");
        }
        resultsMap.clear();
        flushCounter = 0;
        latch.countDown();
        for (Tuple t : tuplesToAck)
          outputCollector.ack(t);
      }
    }
    else
    {
      if (resultsMap.containsKey(colIndex))
      {
        colVal2 = resultsMap.get(colIndex);
        colMult = colVal.multiply(colVal2).mod(nSquared);
        resultsMap.put(colIndex, colMult);
      }
      else
      {
        resultsMap.put(colIndex, colVal);
      }
      logger.debug("column = " + colIndex + ", value = " + resultsMap.get(colIndex).toString());
    }
    outputCollector.ack(tuple);

  }

  @Override public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
  }
}
