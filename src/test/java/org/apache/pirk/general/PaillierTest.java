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
package org.apache.pirk.general;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.Random;

import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic test functionality for Paillier library
 * 
 */
public class PaillierTest
{
  private static final Logger logger = LoggerFactory.getLogger(PaillierTest.class);

  private static BigInteger p = null; // large prime
  private static BigInteger q = null; // large prime
  private static BigInteger N = null; // N=pq, RSA modulus
  private static BigInteger NSquared = null; // N^2
  private static BigInteger lambdaN = null; // lambda(N) = lcm(p-1,q-1)

  private static int bitLength = 0; // bit length of the modulus N
  private static int certainty = 64; // prob that new BigInteger values represents primes will exceed (1 - (1/2)^certainty)

  private static BigInteger r1 = null; // random number in (Z/NZ)*
  private static BigInteger r2 = null; // random number in (Z/NZ)*

  private static BigInteger m1 = null; // message to encrypt
  private static BigInteger m2 = null; // message to encrypt

  @BeforeClass
  public static void setup()
  {
    p = BigInteger.valueOf(7);
    q = BigInteger.valueOf(17);
    N = p.multiply(q);
    NSquared = N.multiply(N);

    lambdaN = BigInteger.valueOf(48);

    r1 = BigInteger.valueOf(3);
    r2 = BigInteger.valueOf(4);

    m1 = BigInteger.valueOf(5);
    m2 = BigInteger.valueOf(2);

    bitLength = 201;// bitLength = 384;
    certainty = 128;

    logger.info("p = " + p.intValue() + " q = " + q.intValue() + " N = " + N.intValue() + " bitLength = " + N.bitLength() + " lambdaN = " + lambdaN + " m1 = "
        + m1.intValue() + " m2 = " + m2.intValue() + " r1 = " + r1.intValue() + " r2 = " + r2.intValue());
  }

  @Test
  @SuppressWarnings("unused")
  public void testPIRExceptions()
  {
    try
    {
      Paillier paillier = new Paillier(BigInteger.valueOf(2), BigInteger.valueOf(2), 128);
      fail("Paillier constructor did not throw PIRException for p,q < 3");
    } catch (PIRException ignore)
    {}

    try
    {
      Paillier paillier = new Paillier(BigInteger.valueOf(2), BigInteger.valueOf(3), 128);
      fail("Paillier constructor did not throw PIRException for p < 3");
    } catch (PIRException ignore)
    {}

    try
    {
      Paillier paillier = new Paillier(BigInteger.valueOf(3), BigInteger.valueOf(2), 128);
      fail("Paillier constructor did not throw PIRException for q < 3");
    } catch (PIRException ignore)
    {}

    try
    {
      Paillier paillier = new Paillier(BigInteger.valueOf(7), BigInteger.valueOf(7), 128);
      fail("Paillier constructor did not throw PIRException for p = q");
    } catch (PIRException ignore)
    {}

    try
    {
      Paillier paillier = new Paillier(BigInteger.valueOf(8), BigInteger.valueOf(7), 128);
      fail("Paillier constructor did not throw PIRException for p not prime");
    } catch (PIRException ignore)
    {}

    try
    {
      Paillier paillier = new Paillier(BigInteger.valueOf(7), BigInteger.valueOf(10), 128);
      fail("Paillier constructor did not throw PIRException for q not prime");
    } catch (PIRException ignore)
    {}

    try
    {
      int systemPrimeCertainty = SystemConfiguration.getIntProperty("pir.primeCertainty", 128);
      Paillier paillier = new Paillier(3072, systemPrimeCertainty - 10);
      fail("Paillier constructor did not throw PIRException for certainty less than system default of " + systemPrimeCertainty);
    } catch (PIRException ignore)
    {}

    try
    {
      Paillier pailler = new Paillier(p, q, bitLength);
      BigInteger encM1 = pailler.encrypt(N);
      fail("Paillier encryption did not throw PIRException for message m = N");
    } catch (PIRException ignore)
    {}

    try
    {
      Paillier pailler = new Paillier(p, q, bitLength);
      BigInteger encM1 = pailler.encrypt(N.add(BigInteger.TEN));
      fail("Paillier encryption did not throw PIRException for message m > N");
    } catch (PIRException ignore)
    {}

    try
    {
      Paillier pailler = new Paillier(bitLength, 128, bitLength);
      fail("Paillier constructor did not throw PIRException for ensureBitSet = bitLength");
    } catch (PIRException ignore)
    {}

    try
    {
      Paillier pailler = new Paillier(bitLength, 128, bitLength + 1);
      fail("Paillier constructor did not throw PIRException for ensureBitSet > bitLength");
    } catch (PIRException ignore)
    {}
  }

  @Test
  public void testPaillierGivenAllParameters() throws Exception
  {
    logger.info("Starting testPaillierGivenAllParameters: ");

    Paillier pailler = new Paillier(p, q, bitLength);

    assertEquals(pailler.getN(), N);
    assertEquals(pailler.getLambdaN(), lambdaN);

    // Check encryption
    BigInteger encM1 = pailler.encrypt(m1, r1);
    BigInteger encM2 = pailler.encrypt(m2, r2);
    logger.info("encM1 = " + encM1.intValue() + " encM2 = " + encM2.intValue());

    assertEquals(encM1, BigInteger.valueOf(14019));
    assertEquals(encM2, BigInteger.valueOf(8836));

    // Check decryption
    BigInteger decM1 = pailler.decrypt(encM1);
    BigInteger decM2 = pailler.decrypt(encM2);
    logger.info("decM1 = " + decM1.intValue() + " decM2 = " + decM2.intValue());

    assertEquals(decM1, m1);
    assertEquals(decM2, m2);

    // Check homomorphic property: E_r1(m1)*E_r2(m2) mod N^2 = E_r1r2((m1+m2) mod N) mod N^2
    BigInteger encM1_times_encM2 = (encM1.multiply(encM2)).mod(NSquared);
    BigInteger encM1plusM2 = pailler.encrypt((m1.add(m2)).mod(N), r1.multiply(r2));
    logger.info("encM1_times_encM2 = " + encM1_times_encM2.intValue() + " encM1plusM2 = " + encM1plusM2.intValue());

    assertEquals(encM1_times_encM2, BigInteger.valueOf(5617));
    assertEquals(encM1plusM2, BigInteger.valueOf(5617));

    logger.info("Successfully completed testPaillierGivenAllParameters: ");
  }

  @Test
  public void testPaillierWithKeyGeneration() throws Exception
  {
    logger.info("Starting testPaillierWithKeyGeneration: ");

    // Test with and without gmp optimization for modPow
    SystemConfiguration.setProperty("pallier.FIPSPrimeGenerationChecks", "true");
    SystemConfiguration.setProperty("paillier.useGMPForModPow", "true");
    SystemConfiguration.setProperty("paillier.GMPConstantTimeMode", "true");
    testPaillerWithKeyGenerationGeneral();

    SystemConfiguration.setProperty("pallier.FIPSPrimeGenerationChecks", "false");

    SystemConfiguration.setProperty("paillier.useGMPForModPow", "true");
    SystemConfiguration.setProperty("paillier.GMPConstantTimeMode", "true");
    testPaillerWithKeyGenerationGeneral();

    SystemConfiguration.setProperty("paillier.useGMPForModPow", "true");
    SystemConfiguration.setProperty("paillier.GMPConstantTimeMode", "false");
    testPaillerWithKeyGenerationGeneral();

    SystemConfiguration.setProperty("paillier.useGMPForModPow", "false");
    SystemConfiguration.setProperty("paillier.GMPConstantTimeMode", "false");
    testPaillerWithKeyGenerationGeneral();

    // Reset the properties
    SystemConfiguration.initialize();

    logger.info("Ending testPaillierWithKeyGeneration: ");
  }

  public void testPaillerWithKeyGenerationGeneral() throws Exception
  {
    // Test without requiring highest bit to be set
    logger.info("Starting testPaillierWithKeyGenerationBitSetOption with ensureHighBitSet = false");
    testPaillierWithKeyGenerationBitSetOption(-1);

    // Test requiring highest bit to be set
    logger.info("Starting testPaillierWithKeyGenerationBitSetOption with ensureHighBitSet = true");
    testPaillierWithKeyGenerationBitSetOption(5);
  }

  public void testPaillierWithKeyGenerationBitSetOption(int ensureBitSet) throws Exception
  {
    Random r = new Random();
    int lowBitLength = 3073; // inclusive
    int highBitLength = 7001; // exclusive

    int loopVal = 1; // int loopVal = 1000; //change this and re-test for high loop testing
    for (int i = 0; i < loopVal; ++i)
    {
      logger.info("i = " + i);

      basicTestPaillierWithKeyGeneration(bitLength, certainty, ensureBitSet);
      basicTestPaillierWithKeyGeneration(3072, certainty, ensureBitSet);

      // Test with random bit length between 3073 and 7000
      int randomLargeBitLength = r.nextInt(highBitLength - lowBitLength) + lowBitLength;
      basicTestPaillierWithKeyGeneration(randomLargeBitLength, certainty, ensureBitSet);
    }
  }

  private void basicTestPaillierWithKeyGeneration(int bitLengthInput, int certaintyInput, int ensureBitSet) throws Exception
  {
    Paillier pailler = new Paillier(bitLengthInput, certaintyInput, ensureBitSet);
    BigInteger generatedN = pailler.getN();
    BigInteger geneartedNsquared = generatedN.multiply(generatedN);

    // Check the decrypting the encryption yields the message
    BigInteger encM1 = pailler.encrypt(m1);
    BigInteger encM2 = pailler.encrypt(m2);
    logger.info("encM1 = " + encM1.intValue() + " encM2 = " + encM2.intValue());

    BigInteger decM1 = pailler.decrypt(encM1);
    BigInteger decM2 = pailler.decrypt(encM2);
    logger.info("decM1 = " + decM1.intValue() + " decM2 = " + decM2.intValue());

    assertEquals(decM1, m1);
    assertEquals(decM2, m2);

    // Check homomorphic property: E_r1(m1)*E_r2(m2) mod N^2 = E_r1r2((m1+m2) mod N) mod N^2
    BigInteger encM1_times_encM2 = (encM1.multiply(encM2)).mod(geneartedNsquared);
    BigInteger multDecrypt = pailler.decrypt(encM1_times_encM2);
    BigInteger m1_plus_m2 = (m1.add(m2)).mod(N);

    logger.info("encM1_times_encM2 = " + encM1_times_encM2.intValue() + " multDecrypt = " + multDecrypt.intValue() + " m1_plus_m2 = " + m1_plus_m2.intValue());

    assertEquals(multDecrypt, m1_plus_m2);
  }
}
