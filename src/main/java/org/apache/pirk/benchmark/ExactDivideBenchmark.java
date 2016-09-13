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

package org.apache.pirk.benchmark;

import java.math.BigInteger;

import org.apache.pirk.encryption.IntegerMathAbstraction;
import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;

public class ExactDivideBenchmark
{
  private static final Logger logger = LoggerFactory.getLogger(ExactDivideBenchmark.class);

  private static final SecureRandom rand = new SecureRandom();

  private static final int FACTOR_SIZE = 3074;

  @State(Scope.Benchmark) public static class ExactDivideBenchmarkState
  {
    BigInteger value1 = null;
    BigInteger value2 = null;
    BigInteger product = null;

    /**
     * This sets up the state for the two separate benchmarks
     */
    @Setup(org.openjdk.jmh.annotations.Level.Trial) public void setUp()
    {
      // Create two very large numbers, multiply them together to get a product that can be divided by either value without a getting a remainder
      value1 = getRandomBigIntegerWithBitSet(FACTOR_SIZE, FACTOR_SIZE - 2);
      value2 = getRandomBigIntegerWithBitSet(FACTOR_SIZE, FACTOR_SIZE - 2);
      product = value1.multiply(value2);
    }
  }

  public static BigInteger getRandomBigIntegerWithBitSet(int bitlength, int bitset)
  {
    BigInteger toReturn = null;
    do
    {
      toReturn = new BigInteger(bitlength, rand);
    } while (!toReturn.testBit(bitset));
    return toReturn;
  }

  @Benchmark @BenchmarkMode(Mode.Throughput) public void testWithGMP(ExactDivideBenchmarkState allState)
  {
    SystemConfiguration.setProperty("paillier.useGMPForExactDivide", "true");
    IntegerMathAbstraction.reloadConfiguration();

    try
    {
      IntegerMathAbstraction.exactDivide(allState.product, allState.value1);
    } catch (Exception e)
    {
      logger.error("Exception in testWithGMP!\n" + e);
      System.exit(1);
    }
  }

  @Benchmark @BenchmarkMode(Mode.Throughput) public void testWithoutGMP(ExactDivideBenchmarkState allState)
  {
    SystemConfiguration.setProperty("paillier.useGMPForExactDivide", "false");
    IntegerMathAbstraction.reloadConfiguration();

    try
    {
      IntegerMathAbstraction.exactDivide(allState.product, allState.value1);
    } catch (Exception e)
    {
      logger.error("Exception in testWithoutGMP!\n" + e);
      System.exit(1);
    }
  }

}
