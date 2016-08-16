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
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.apache.pirk.serialization.LocalFileSystemStore;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Bolt to compute and output the final Response object for a query
 * <p>
 * Receives {@code <colIndex, colProduct>} tuples, computes the final column product for each colIndex, records the results in the final Response object, and
 * outputs the final Response object for the query.
 * <p>
 * Flush signals are sent to the OuputBolt from the EncColMultBolts via a tuple of the form {@code <-1, 0>}. Once a flush signal has been received from each
 * EncColMultBolt (or a timeout is reached), the final column product is computed and the final Response is formed and emitted.
 * <p>
 * Currently, the Responses are written to HDFS to location specified by the outputFile with the timestamp appended.
 * <p>
 * TODO: -- Enable other Response output locations
 * 
 */
public class OutputBolt extends BaseRichBolt
{
  private static final long serialVersionUID = 1L;

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(OutputBolt.class);

  private OutputCollector outputCollector;
  private QueryInfo queryInfo;
  private Response response;
  private String outputFile;
  private boolean hdfs;
  private String hdfsUri;
  private Integer flushCounter = 0;
  private ArrayList<Tuple> tuplesToAck = new ArrayList<Tuple>();
  private Integer totalFlushSigs;

  private LocalFileSystemStore localStore;
  private HadoopFileSystemStore hadoopStore;

  // This latch just serves as a hook for testing.
  public static CountDownLatch latch = new CountDownLatch(1);

  // This is the main object here. It holds column Id -> product
  private HashMap<Long,BigInteger> resultsMap = new HashMap<Long,BigInteger>();

  private BigInteger colVal;
  private BigInteger colMult;

  private BigInteger nSquared;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector)
  {
    outputCollector = collector;

    totalFlushSigs = ((Long) map.get(StormConstants.ENCCOLMULTBOLT_PARALLELISM_KEY)).intValue();
    outputFile = (String) map.get(StormConstants.OUTPUT_FILE_KEY);

    hdfs = (boolean) map.get(StormConstants.USE_HDFS);
    logger.info("output databolt hdfs = " + map.get(StormConstants.USE_HDFS));

    if (hdfs)
    {
      hdfsUri = (String) map.get(StormConstants.HDFS_URI_KEY);
      try
      {
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
        hadoopStore = new HadoopFileSystemStore(fs);
      } catch (IOException e)
      {
        logger.error("Failed to initialize Hadoop file system for output.");
        throw new RuntimeException(e);
      }
    }
    else
    {
      localStore = new LocalFileSystemStore();
    }
    nSquared = new BigInteger((String) map.get(StormConstants.N_SQUARED_KEY));
    queryInfo = new QueryInfo((Map) map.get(StormConstants.QUERY_INFO_KEY));
    response = new Response(queryInfo);

    logger.info("Intitialized OutputBolt.");
  }

  @Override
  public void execute(Tuple tuple)
  {
    long colIndex = tuple.getLongByField(StormConstants.COLUMN_INDEX_ECM_FIELD);
    colVal = (BigInteger) tuple.getValueByField(StormConstants.COLUMN_PRODUCT_FIELD);

    // colIndex == -1 is just the signal sent by EncColMultBolt to notify that it flushed it's values.
    // Could have created a new stream for such signals, but that seemed like overkill.
    if (colIndex == -1)
    {
      flushCounter++;

      logger.debug("Received " + flushCounter + " output flush signals out of " + totalFlushSigs);

      // Wait till all EncColMultBolts have been flushed
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
            hadoopStore.store(new Path(outputFile + "_" + timestamp), response);
          }
          else
          { // In order to accommodate testing, this does not currently include timestamp.
            // Should probably be fixed, but this will not likely be used outside of testing.
            localStore.store(new File(outputFile), response);
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

        // Reset
        resultsMap.clear();
        flushCounter = 0;
        for (Tuple t : tuplesToAck)
          outputCollector.ack(t);
        // Used for integration test
        latch.countDown();
      }
    }
    else
    {
      // Process data values: add them to map. The column multiplication is only done in the case where saltColumns==true,
      // in which case a small number of multiplications still need to be done per column.
      if (resultsMap.containsKey(colIndex))
      {
        colMult = colVal.multiply(resultsMap.get(colIndex)).mod(nSquared);
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

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {}
}
