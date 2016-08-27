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

package org.apache.pirk.responder.wideskies.storm;

import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.responder.wideskies.common.ComputeEncryptedRow;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Bolt class to perform the encrypted row calculation
 * <p/>
 * Receives a {@code <hash(selector), dataPartitions>} tuple as input.
 * <p/>
 * Encrypts the row data and emits a (column index, encrypted row-value) tuple for each encrypted block.
 * <p/>
 * Every FLUSH_FREQUENCY seconds, it sends a signal to EncColMultBolt to flush its output and resets all counters. At that point, all outgoing (column index,
 * encrypted row-value) tuples are buffered until a SESSION_END signal is received back from the EncColMultBolt.
 */
public class EncRowCalcBolt extends BaseRichBolt
{
  private static final long serialVersionUID = 1L;

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EncRowCalcBolt.class);

  private OutputCollector outputCollector;
  private static Query query;
  private static boolean querySet = false;

  private Boolean limitHitsPerSelector;
  private Long maxHitsPerSelector;
  private Long totalEndSigs;
  private int rowDivisions;
  private Boolean saltColumns;
  private Boolean splitPartitions;

  private Random rand;

  // These are the main data structures used here.
  private Map<Integer,Integer> hitsByRow = new HashMap<Integer,Integer>();
  private Map<Integer,Integer> colIndexByRow = new HashMap<Integer,Integer>();
  private List<Tuple2<Long,BigInteger>> matrixElements = new ArrayList<Tuple2<Long,BigInteger>>();
  private List<BigInteger> dataArray = new ArrayList<>();

  private int numEndSigs = 0;

  // These buffered values are used in the case when a session has been ejected, but the SESSION_END signal has not been received
  // yet from the next bolt.
  private boolean buffering = false;
  private List<Tuple2<Long,BigInteger>> bufferedValues = new ArrayList<Tuple2<Long,BigInteger>>();

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector coll)
  {
    outputCollector = coll;
    setQuery(map);
    logger.info("partition databolt hdfs = " + map.get(StormConstants.USE_HDFS));

    maxHitsPerSelector = (Long) map.get(StormConstants.MAX_HITS_PER_SEL_KEY);
    limitHitsPerSelector = (Boolean) map.get(StormConstants.LIMIT_HITS_PER_SEL_KEY);
    totalEndSigs = (Long) map.get(StormConstants.ENCCOLMULTBOLT_PARALLELISM_KEY);
    splitPartitions = (Boolean) map.get(StormConstants.SPLIT_PARTITIONS_KEY);
    saltColumns = (Boolean) map.get(StormConstants.SALT_COLUMNS_KEY);
    rowDivisions = ((Long) map.get(StormConstants.ROW_DIVISIONS_KEY)).intValue();

    // If splitPartitions==true, the data is incoming partition by partition, rather than record by record.
    // The numRecords below will increment every partition elt exceed the maxHitsPerSelector param far too
    // soon unless the latter is modified.
    if (splitPartitions)
      maxHitsPerSelector *= query.getQueryInfo().getNumPartitionsPerDataElement();

    rand = new Random();

    logger.info("Initialized EncRowCalcBolt.");
  }

  @Override
  public void execute(Tuple tuple)
  {
    if (tuple.getSourceStreamId().equals(StormConstants.DEFAULT))
    {
      matrixElements = processTupleFromPartitionDataBolt(tuple); // tuple: <hash,partitions>

      if (buffering)
      {
        logger.debug("Buffering tuple.");
        bufferedValues.addAll(matrixElements);
      }
      else
      {
        emitTuples(matrixElements);
      }
    }
    else if (StormUtils.isTickTuple(tuple) && !buffering)
    {
      logger.debug("Sending flush signal to EncColMultBolt.");
      outputCollector.emit(StormConstants.ENCROWCALCBOLT_FLUSH_SIG, new Values(1));

      colIndexByRow.clear();
      hitsByRow.clear();

      buffering = true;
    }
    else if (tuple.getSourceStreamId().equals(StormConstants.ENCCOLMULTBOLT_SESSION_END))
    {
      numEndSigs += 1;
      logger.debug("SessionEnd signal {} of {} received", numEndSigs, totalEndSigs);

      // Need to receive signal from all EncColMultBolt instances before stopping buffering.
      if (numEndSigs == totalEndSigs)
      {
        logger.debug("Buffering completed, emitting {} tuples.", bufferedValues.size());
        emitTuples(bufferedValues);
        bufferedValues.clear();
        buffering = false;

        numEndSigs = 0;
      }
    }
    outputCollector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declareStream(StormConstants.ENCROWCALCBOLT_DATASTREAM_ID,
        new Fields(StormConstants.COLUMN_INDEX_ERC_FIELD, StormConstants.ENCRYPTED_VALUE_FIELD, StormConstants.SALT));
    outputFieldsDeclarer.declareStream(StormConstants.ENCROWCALCBOLT_FLUSH_SIG, new Fields(StormConstants.FLUSH));
  }

  /***
   * Extracts (hash, data partitions) from tuple. Encrypts the data partitions. Returns all of the pairs of (col index, col value). Also advances the
   * colIndexByRow and hitsByRow appropriately.
   *
   * @param tuple
   * @return
   */
  private List<Tuple2<Long,BigInteger>> processTupleFromPartitionDataBolt(Tuple tuple)
  {
    matrixElements.clear();
    int rowIndex = tuple.getIntegerByField(StormConstants.HASH_FIELD);

    if (!colIndexByRow.containsKey(rowIndex))
    {
      colIndexByRow.put(rowIndex, 0);
      hitsByRow.put(rowIndex, 0);
    }

    if (splitPartitions)
    {
      dataArray.add((BigInteger) tuple.getValueByField(StormConstants.PARTIONED_DATA_FIELD));
    }
    else
    {
      dataArray = (ArrayList<BigInteger>) tuple.getValueByField(StormConstants.PARTIONED_DATA_FIELD);
    }
    logger.debug("Retrieving {} elements in EncRowCalcBolt.", dataArray.size());

    try
    {
      int colIndex = colIndexByRow.get(rowIndex);
      int numRecords = hitsByRow.get(rowIndex);

      if (limitHitsPerSelector && numRecords < maxHitsPerSelector)
      {
        logger.debug("computing matrix elements.");
        matrixElements = ComputeEncryptedRow.computeEncRow(dataArray, query, rowIndex, colIndex);
        colIndexByRow.put(rowIndex, colIndex + matrixElements.size());
        hitsByRow.put(rowIndex, numRecords + 1);
      }
      else if (limitHitsPerSelector)
      {
        logger.info("maxHits: rowIndex = " + rowIndex + " elementCounter = " + numRecords);
      }
    } catch (IOException e)
    {
      logger.warn("Caught IOException while encrypting row. ", e);
    }

    dataArray.clear();
    return matrixElements;
  }

  private void emitTuples(List<Tuple2<Long,BigInteger>> matrixElements)
  {
    // saltColumns distributes the column multiplication done in the next bolt EncColMultBolt to avoid hotspotting.
    if (saltColumns)
    {
      for (Tuple2<Long,BigInteger> sTuple : matrixElements)
      {
        outputCollector.emit(StormConstants.ENCROWCALCBOLT_DATASTREAM_ID, new Values(sTuple._1(), sTuple._2(), rand.nextInt(rowDivisions)));
      }
    }
    else
    {
      for (Tuple2<Long,BigInteger> sTuple : matrixElements)
      {
        outputCollector.emit(StormConstants.ENCROWCALCBOLT_DATASTREAM_ID, new Values(sTuple._1(), sTuple._2(), 0));
      }
    }
  }

  private synchronized static void setQuery(Map map)
  {
    if (!querySet)
    {
      query = StormUtils.prepareQuery(map);
      querySet = true;
    }
  }
}
