/*******************************************************************************
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
 *******************************************************************************/

package org.apache.pirk.benchmark;

import com.google.caliper.BeforeExperiment;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;

import java.math.BigInteger;

import org.apache.log4j.Logger;
import org.apache.pirk.encryption.ModPowAbstraction;
import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.utils.LogUtils;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;

/**
 * The purpose of these Pallier Benchmarks is to compare the performance of
 * Mod Pow using BigInteger's implementation and gmp's via Square's JNI connector.
 * <p>
 * mvn exec:java Dexec.mainClass="com.google.caliper.runner.CaliperMain" Dexec.args="org.apache.pirk.benchmark.PaillierBenchmark"
 */
public class PaillierBenchmark
{

  private static BigInteger r1, m1;
  private static Paillier paillier;
  private static Logger logger = LogUtils.getLoggerForThisClass();
  private static final int MODULUS_SIZE = 3074;

  static
  {
    int systemPrimeCertainty = Integer.parseInt(SystemConfiguration.getProperty("pir.primeCertainty", "100"));
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

  // public static void main(String[] args) throws Exception {
  //   new Runner().run(SetBenchmark.class.getName());
  // }

  @Benchmark public void timePallierWithGMP(int reps)
  {
    SystemConfiguration.setProperty("paillier.useGMPForModPow", "true");
    SystemConfiguration.setProperty("paillier.GMPConstantTimeMode", "false");
    ModPowAbstraction.reloadConfiguration();

    try
    {
      for (int i = 0; i < reps; i++)
      {
        paillier.encrypt(m1, r1);
      }
    } catch (PIRException e)
    {
      logger.info("Exception in testWithGMP!\n");
    }
  }

  @Benchmark public void timePallierWithGMPConstantTime(int reps)
  {
    SystemConfiguration.setProperty("paillier.useGMPForModPow", "true");
    SystemConfiguration.setProperty("paillier.GMPConstantTimeMode", "true");
    ModPowAbstraction.reloadConfiguration();

    try
    {
      for (int i = 0; i < reps; i++)
      {
        paillier.encrypt(m1, r1);
      }
    } catch (PIRException e)
    {
      logger.info("Exception in testWithGMP!\n");
    }
  }

  @Benchmark public void timePallierWithoutGMP(int reps)
  {
    SystemConfiguration.setProperty("paillier.useGMPForModPow", "false");
    SystemConfiguration.setProperty("paillier.GMPConstantTimeMode", "false");
    ModPowAbstraction.reloadConfiguration();

    try
    {
      for (int i = 0; i < reps; i++)
      {
        paillier.encrypt(m1, r1);
      }
    } catch (PIRException e)
    {
      logger.info("Exception in testWithGMP!\n");
    }
  }

}
