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

public class ModularInverseBenchmark
{
  private static final Logger logger = LoggerFactory.getLogger(PaillierBenchmark.class);
  // Since the results of this are only used for benchmarking and not for any actual
  // encryption we do not care if the results are any good.
  private static final SecureRandom rand = new SecureRandom();

  private static final int MODULUS_SIZE = 3074;
  private static final int FACTOR_SIZE = MODULUS_SIZE * 2;

  @State(Scope.Benchmark) public static class ModularInverseBenchmarkState
  {
    // Instantiate a Paillier object so we can grab the N-squared from it: 
    Paillier paillier = null;

    BigInteger factor1 = null;
    BigInteger modulus = null;

    /**
     * This sets up the state for the two separate benchmarks
     */
    @Setup(org.openjdk.jmh.annotations.Level.Trial) public void setUp()
    {
      int systemPrimeCertainty = Integer.parseInt(SystemConfiguration.getProperty("pir.primeCertainty", "100"));
      try
      {
        // Create a new large number with the second highest bit definitely set
        paillier = new Paillier(MODULUS_SIZE, systemPrimeCertainty, MODULUS_SIZE - 2);
      } catch (PIRException e)
      {
        System.out.printf("Couldn't build paillier object!\n");
      }

      factor1 = ModularMultiplyBenchmark.getRandomBigIntegerWithBitSet(FACTOR_SIZE, FACTOR_SIZE - 2);
      modulus = paillier.getNSquared();
    }
  }

  @Benchmark @BenchmarkMode(Mode.Throughput) public void testWithGMP(ModularInverseBenchmarkState allState)
  {
    SystemConfiguration.setProperty("paillier.useGMPForModularInverse", "true");
    IntegerMathAbstraction.reloadConfiguration();

    try
    {
      IntegerMathAbstraction.modInverse(allState.factor1, allState.modulus);
    } catch (Exception e)
    {
      logger.error("Exception in testWithGMP!\n" + e);
      System.exit(1);
    }
  }

  @Benchmark @BenchmarkMode(Mode.Throughput) public void testWithoutGMP(ModularInverseBenchmarkState allState)
  {
    SystemConfiguration.setProperty("paillier.useGMPForModularInverse", "false");
    IntegerMathAbstraction.reloadConfiguration();

    try
    {
      IntegerMathAbstraction.modInverse(allState.factor1, allState.modulus);
    } catch (Exception e)
    {
      logger.error("Exception in testWithoutGMP!\n" + e);
      System.exit(1);
    }
  }

}
