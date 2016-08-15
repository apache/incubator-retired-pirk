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

/**
 * A JMH benchmark to evaluate Paillier performance both with and without using com.square.jnagmp.gmp to accelerate modPow
 * <p>
 * Guides to using JMH can be found at: http://tutorials.jenkov.com/java-performance/jmh.html and http://nitschinger.at/Using-JMH-for-Java-Microbenchmarking/
 */

public class PaillierBenchmark
{
  private static final int MODULUS_SIZE = 3074;
  private static final Logger logger = LoggerFactory.getLogger(PaillierBenchmark.class);

  @State(Scope.Benchmark)
  public static class PaillierBenchmarkState
  {
    BigInteger r1 = null; // random number in (Z/NZ)*
    BigInteger m1 = null; // message to encrypt

    Paillier paillier = null;

    /**
     * This sets up the state for the two separate benchmarks
     */
    @Setup(org.openjdk.jmh.annotations.Level.Trial)
    public void setUp()
    {
      int systemPrimeCertainty = SystemConfiguration.getIntProperty("pir.primeCertainty", 100);
      try
      {
        paillier = new Paillier(MODULUS_SIZE, systemPrimeCertainty);

      } catch (PIRException e)
      {
        System.out.printf("Couldn't build paillier object!\n");
      }

      r1 = BigInteger.valueOf(3);
      m1 = BigInteger.valueOf(5);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void testWithGMP(PaillierBenchmarkState allState)
  {
    SystemConfiguration.setProperty("paillier.useGMPForModPow", "true");
    SystemConfiguration.setProperty("paillier.GMPConstantTimeMode", "false");
    IntegerMathAbstraction.reloadConfiguration();

    try
    {
      allState.paillier.encrypt(allState.m1, allState.r1);
    } catch (PIRException e)
    {
      logger.info("Exception in testWithGMP!\n");
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void testWithGMPConstantTime(PaillierBenchmarkState allState)
  {
    SystemConfiguration.setProperty("paillier.useGMPForModPow", "true");
    SystemConfiguration.setProperty("paillier.GMPConstantTimeMode", "true");
    IntegerMathAbstraction.reloadConfiguration();

    try
    {
      allState.paillier.encrypt(allState.m1, allState.r1);
    } catch (PIRException e)
    {
      logger.info("Exception in testWithGMPConstantTime!\n");
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void testWithoutGMP(PaillierBenchmarkState allState)
  {
    SystemConfiguration.setProperty("paillier.useGMPForModPow", "false");
    IntegerMathAbstraction.reloadConfiguration();

    try
    {
      allState.paillier.encrypt(allState.m1, allState.r1);
    } catch (PIRException e)
    {
      logger.info("Exception in testWithoutGMP!\n");
    }
  }
}
