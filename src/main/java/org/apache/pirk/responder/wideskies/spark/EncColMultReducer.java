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
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function to perform encrypted column multiplication
 *
 */
public class EncColMultReducer implements Function2<BigInteger,BigInteger,BigInteger>
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(EncColMultReducer.class);

  Query query = null;

  public EncColMultReducer(BroadcastVars bbVarsIn)
  {

    query = bbVarsIn.getQuery();

    logger.info("Initialized EncColMultReducer");
  }

  @Override
  public BigInteger call(BigInteger colVal1, BigInteger colVal2) throws Exception
  {
    // long startTime = System.currentTimeMillis();

    BigInteger colMult = (colVal1.multiply(colVal2)).mod(query.getNSquared());

    logger.debug("colVal1 = " + colVal1.toString() + " colVal2 = " + colVal2.toString() + " colMult = " + colMult.toString());

    // long endTime = System.currentTimeMillis();
    // logger.info("Completed column mult: duration = " + (endTime-startTime));

    return colMult;
  }
}
