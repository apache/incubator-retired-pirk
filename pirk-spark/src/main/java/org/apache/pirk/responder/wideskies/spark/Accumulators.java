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

import java.io.Serializable;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accumulators for the Responder
 *
 */
public class Accumulators implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(Accumulators.class);

  private Accumulator<Integer> numRecordsReceived = null;
  private Accumulator<Integer> numRecordsFiltered = null;
  private Accumulator<Integer> numRecordsAfterFilter = null;
  private Accumulator<Integer> numHashes = null;
  private Accumulator<Integer> numColumns = null;
  private Accumulator<Integer> numBatches = null;

  public Accumulators(JavaSparkContext sc)
  {
    numRecordsReceived = sc.accumulator(0);
    numRecordsFiltered = sc.accumulator(0);
    numRecordsAfterFilter = sc.accumulator(0);
    numHashes = sc.accumulator(0);
    numColumns = sc.accumulator(0);
    numBatches = sc.accumulator(0);
  }

  public Integer numRecordsReceivedGetValue()
  {
    return numRecordsReceived.value();
  }

  public void incNumRecordsReceived(int val)
  {
    numRecordsReceived.add(val);
  }

  public Integer numRecordsFilteredGetValue()
  {
    return numRecordsFiltered.value();
  }

  public void incNumRecordsFiltered(int val)
  {
    numRecordsFiltered.add(val);
  }

  public Integer numRecordsRecordsAfterFilterGetValue()
  {
    return numRecordsAfterFilter.value();
  }

  public void incNumRecordsAfterFilter(int val)
  {
    numRecordsAfterFilter.add(val);
  }

  public Integer numHashesGetValue()
  {
    return numHashes.value();
  }

  public void incNumHashes(int val)
  {
    numHashes.add(val);
  }

  public Integer numColumnsGetValue()
  {
    return numColumns.value();
  }

  public void incNumColumns(int val)
  {
    numColumns.add(val);
  }

  public Integer numBatchesGetValue()
  {
    return numBatches.value();
  }

  public void incNumBatches(int val)
  {
    numBatches.add(val);
  }

  public void resetAll()
  {
    numRecordsReceived.setValue(0);
    numRecordsFiltered.setValue(0);
    numRecordsAfterFilter.setValue(0);
    numHashes.setValue(0);
    numColumns.setValue(0);
    numBatches.setValue(0);
  }

  public void printAll()
  {
    logger.info("numRecordsReceived = " + numRecordsReceived.value() + " \n numRecordsFiltered = " + numRecordsFiltered + " \n numRecordsAfterFilter = "
        + numRecordsAfterFilter + " \n numHashes = " + numHashes + " \n numColumns = " + numColumns.value() + " \n numBatches = " + numBatches.value());
  }
}
