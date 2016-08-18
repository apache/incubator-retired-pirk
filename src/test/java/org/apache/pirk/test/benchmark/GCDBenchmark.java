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

public class GCDBenchmark
{
  private static final Logger logger = LoggerFactory.getLogger(GCDBenchmark.class);

  private static final SecureRandom rand = new SecureRandom();

  private static final int FACTOR_SIZE = 3074;

  @State(Scope.Benchmark) public static class GCDBenchmarkState
  {
    BigInteger common  = null;
    BigInteger value1 = null;
    BigInteger value2 = null;

    /**
     * This sets up the state for the two separate benchmarks
     */
    @Setup(org.openjdk.jmh.annotations.Level.Trial) public void setUp()
    {
      // Create two very large numbers with a common factor. 
      common = getRandomBigIntegerWithBitSet(FACTOR_SIZE, FACTOR_SIZE - 2);
      value1 = common.multiply(getRandomBigIntegerWithBitSet(FACTOR_SIZE, FACTOR_SIZE - 2));
      value2 = common.multiply(getRandomBigIntegerWithBitSet(FACTOR_SIZE, FACTOR_SIZE - 2));
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

  @Benchmark @BenchmarkMode(Mode.Throughput) public void testWithGMP(GCDBenchmarkState allState)
  {
    SystemConfiguration.setProperty("paillier.useGMPForGCD", "true");
    IntegerMathAbstraction.reloadConfiguration();

    try
    {
      IntegerMathAbstraction.gcd(allState.value1, allState.value2);
    } catch (Exception e)
    {
      logger.error("Exception in testWithGMP!\n" + e);
      System.exit(1);
    }
  }

  @Benchmark @BenchmarkMode(Mode.Throughput) public void testWithoutGMP(GCDBenchmarkState allState)
  {
    SystemConfiguration.setProperty("paillier.useGMPForGCD", "false");
    IntegerMathAbstraction.reloadConfiguration();

    try
    {
      allState.value1.gcd(allState.value2);
    } catch (Exception e)
    {
      logger.error("Exception in testWithoutGMP!\n" + e);
      System.exit(1);
    }
  }

}
