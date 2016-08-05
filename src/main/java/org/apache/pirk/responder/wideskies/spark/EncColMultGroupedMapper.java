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

import java.math.BigInteger;

import org.apache.pirk.query.wideskies.Query;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.pirk.encryption.IntegerMathAbstraction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Class for performing encrypted column multiplication when all columns haven been grouped by column number
 * 
 */
public class EncColMultGroupedMapper implements PairFunction<Tuple2<Long,Iterable<BigInteger>>,Long,BigInteger>
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(EncColMultGroupedMapper.class);

  Query query = null;

  EncColMultGroupedMapper(Accumulators accumIn, BroadcastVars bbVarsIn)
  {

    query = bbVarsIn.getQuery();

    logger.info("Initialized EncColMultReducer");
  }

  @Override
  public Tuple2<Long,BigInteger> call(Tuple2<Long,Iterable<BigInteger>> colVals) throws Exception
  {
    // long startTime = System.currentTimeMillis();

    BigInteger colVal = BigInteger.ONE;
    for (BigInteger col : colVals._2)
    {
      colVal = IntegerMathAbstraction.modularMultiply(colVal, col, query.getNSquared());
    }

    // long endTime = System.currentTimeMillis();
    // logger.debug("Completed column mult for col = " + colVals._1 + " duration = " + (endTime - startTime));

    return new Tuple2<>(colVals._1, colVal);
  }
}
