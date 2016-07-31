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
package org.apache.pirk.encryption;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Random;

import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to generate the primes used in the Paillier cryptosystem
 * <p>
 * This class will either:
 * <p>
 * (1) Generate the primes according to Java's BigInteger prime generation methods, which satisfy ANSI X9.80
 * <p>
 * or
 * <p>
 * (2) Bolster Java BigInteger's prime generation to meet the requirements of NIST SP 800-56B ("Recommendation for Pair-Wise Key Establishment Schemes Using
 * Integer Factorization Cryptography") and FIPS 186-4 ("Digital Signature Standard (DSS)") for key generation using probable primes.
 * <p>
 * Relevant page: SP 800-56B: p30 http://csrc.nist.gov/publications/nistpubs/800-56B/sp800-56B.pdf#page=30 Heading: 5.4 Prime Number Generators
 * <p>
 * Relevant pages FIPS 186-4: p50-p53, p55, p71: Sections B.3.1, B.3.3
 * <p>
 * http://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.186-4.pdf#page=61
 * <p>
 * http://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.186-4.pdf#page=80
 * <p>
 * Headings of most interest: Table C.2 "Minimum number of rounds of M-R testing when generating primes for use in RSA Digital Signatures" and "The primes p and
 * q shall be selected with the following constraints"
 * 
 */
public class PrimeGenerator
{
  private static final Logger logger = LoggerFactory.getLogger(PrimeGenerator.class);

  private static final HashMap<Integer,BigInteger> lowerBoundCache = new HashMap<>();
  private static final HashMap<Integer,BigInteger> minimumDifferenceCache = new HashMap<>();

  private static boolean additionalChecksEnabled = SystemConfiguration.getProperty("pallier.FIPSPrimeGenerationChecks").equals("true");

  /**
   * Method to generate a single prime
   * <p>
   * Will optionally ensure that the prime meets the requirements in NIST SP 800-56B and FIPS 186-4
   * <p>
   * NOTE: bitLength corresponds to the FIPS 186-4 nlen parameter
   */
  public static BigInteger getSinglePrime(int bitLength, int certainty, Random rnd)
  {
    BigInteger p;

    logger.debug("bitLength " + bitLength + " certainty " + certainty + " random " + rnd);

    // If the additional checks are disabled, don't waste time on anything else
    if (!additionalChecksEnabled)
    {
      p = new BigInteger(bitLength / 2, certainty, rnd);
    }
    else
    {
      // Calculate the number of Miller-Rabin rounds that we need in addition to
      // the number that BigInteger will do to comply with FIPS 186-4, Appendix C.2
      int roundsLeft = calcNumAdditionalMillerRabinRounds(bitLength);

      // Calculate the lower bound (\sqrt(2))(2^(bitLength/2) – 1)) for use in FIPS 186-4 B.3.3, step 4.4
      BigInteger lowerBound;
      if (!lowerBoundCache.containsKey(bitLength))
      {
        lowerBound = BigDecimal.valueOf(Math.sqrt(2)).multiply(BigDecimal.valueOf(2).pow((bitLength / 2) - 1)).toBigInteger();
        lowerBoundCache.put(bitLength, lowerBound);
      }
      else
      {
        lowerBound = lowerBoundCache.get(bitLength);
      }

      // Complete FIPS 186-4 B.3.3, steps 4.2 - 4.5
      while (true)
      {
        // FIPS 186-4 B.3.3, step 4.2 and 4.3
        // 4.2: Obtain a string p of (bitLength/2) bits from an RBG that supports the security_strength.
        // 4.3: If (p is not odd), then p = p + 1. -- BigInteger hands us a prime (hence odd) with the call below
        p = new BigInteger(bitLength / 2, certainty, rnd);

        // FIPS 186-4 B.3.3, step 4.4
        // 4.4: If p < (\sqrt(2))(2^(bitLength/2) – 1)), then go to step 4.2.
        if (p.compareTo(lowerBound) > -1)
        {
          // FIPS 186-4, step 4.5
          // Run however many more rounds of Miller-Rabin are needed
          if (passesMillerRabin(p, roundsLeft, rnd))
          {
            // We have winner
            break;
          }
        }
      }
    }
    return p;
  }

  /**
   * Method to generate a second prime, q, in relation to a (p,q) RSA key pair
   * <p>
   * Will optionally ensure that the prime meets the requirements in NIST SP 800-56B and FIPS 186-4
   * <p>
   * NOTE: bitLength corresponds to the FIPS 186-4 nlen parameter
   */
  public static BigInteger getSecondPrime(int bitLength, int certainty, Random rnd, BigInteger p)
  {
    BigInteger q;

    logger.debug("bitLength " + bitLength + " certainty " + certainty + " random " + rnd);

    // If the additional checks are disabled, don't waste time on anything else
    if (!additionalChecksEnabled)
    {
      q = new BigInteger(bitLength / 2, certainty, rnd);
    }
    else
    {
      // Calculate the number of Miller-Rabin rounds that we need in addition to
      // the number that BigInteger will do to comply with FIPS 186-4, Appendix C.2
      int roundsLeft = calcNumAdditionalMillerRabinRounds(bitLength);

      // Calculate the lower bound (\sqrt(2))(2^(bitLength/2) – 1)) for use in FIPS 186-4 B.3.3, step 5.5
      BigInteger lowerBound;
      if (!lowerBoundCache.containsKey(bitLength))
      {
        lowerBound = BigDecimal.valueOf(Math.sqrt(2)).multiply(BigDecimal.valueOf(2).pow((bitLength / 2) - 1)).toBigInteger();
        lowerBoundCache.put(bitLength, lowerBound);
      }
      else
      {
        lowerBound = lowerBoundCache.get(bitLength);
      }

      // Compute the minimumDifference 2^((bitLength/2) – 100) for use in FIPS 186-4 B.3.3, step 5.4
      BigInteger minimumDifference;
      if (!minimumDifferenceCache.containsKey(bitLength))
      {
        minimumDifference = BigDecimal.valueOf(2).pow(bitLength / 2 - 100).toBigInteger();
        minimumDifferenceCache.put(bitLength, minimumDifference);
      }
      else
      {
        minimumDifference = minimumDifferenceCache.get(bitLength);
      }

      // Complete FIPS 186-4 B.3.3, steps 5.2 - 5.6
      while (true)
      {
        // FIPS 186-4 B.3.3, step 5.2 and 5.3
        // 5.2: Obtain a string q of (bitLength/2) bits from an RBG that supports the security_strength.
        // 5.3: If (q is not odd), then q = q + 1. -- BigInteger hands us a prime (hence odd) with the call below
        q = new BigInteger(bitLength / 2, certainty, rnd);

        // FIPS 186-4 B.3.3, step 5.4 & 5.5
        // 5.4 If (|p – q| ≤ 2^((bitLength/2) – 100), then go to step 5.2
        // 5.5: If q < (\sqrt(2))(2^(bitLength/2) – 1)), then go to step 5.2.
        BigInteger absDiff = (p.subtract(q)).abs();
        if ((q.compareTo(lowerBound) > -1) && (absDiff.compareTo(minimumDifference) > 0))
        {
          // FIPS 186-4, step 5.6
          // Run however many more rounds of Miller-Rabin are needed
          if (passesMillerRabin(q, roundsLeft, rnd))
          {
            // We have winner
            break;
          }
        }
      }
    }
    return q;
  }

  /**
   * This method returns a two-long array containing a viable RSA p and q meeting FIPS 186-4 and SP 800-56B
   */
  public static BigInteger[] getPrimePair(int bitLength, int certainty, Random rnd)
  {
    BigInteger[] toReturn = {null, null};

    toReturn[0] = getSinglePrime(bitLength, certainty, rnd);
    toReturn[1] = getSecondPrime(bitLength, certainty, rnd, toReturn[0]);

    return toReturn;
  }

  /**
   * Method to return the number of Miller-Rabin rounds that we need in addition to those that BigInteger will do
   */
  private static int calcNumAdditionalMillerRabinRounds(int bitLength)
  {
    // The code in BigInteger.java:primeToCertainty(...) that selects the number of MR rounds is excerpted below:
    // if (sizeInBits < 100) { rounds = 50; rounds = n < rounds ? n : rounds; return passesMillerRabin(rounds, random); }
    // if (sizeInBits < 256) { rounds = 27; }
    // else if (sizeInBits < 512) { rounds = 15; }
    // else if (sizeInBits < 768) { rounds = 8; }
    // else if (sizeInBits < 1024) { rounds = 4; }
    // else { rounds = 2; } rounds = n < rounds ? n : rounds;
    int roundsLeft = 0;

    if (bitLength >= 1536)
    {
      roundsLeft = 2; // BigInteger prime generation performs 2 rounds of MR tests; 4 is FIPS 186-4 compliant
    }
    else if (bitLength >= 1024)
    {
      roundsLeft = 3; // BigInteger prime generation performs 2 rounds of MR tests; 5 is FIPS 186-4 compliant
    }

    return roundsLeft;
  }

  /**
   * Returns true iff this BigInteger passes the specified number of Miller-Rabin tests.
   * <p>
   * This test is taken from the FIPS 186-4, C.3.1
   * <p>
   * The following assumptions are made:
   * <p>
   * This BigInteger is a positive, odd number greater than 2. iterations<=50.
   */
  private static boolean passesMillerRabin(BigInteger w, int iterations, Random rnd)
  {
    // Find a and m:
    // a is the largest int such that 2^a | (w-1)
    // and m = (w-1) / 2^a
    BigInteger wMinusOne = w.subtract(BigInteger.ONE);
    BigInteger m = wMinusOne;
    int a = m.getLowestSetBit();
    m = m.shiftRight(a);

    for (int i = 0; i < iterations; i++)
    {
      // Generate a random b, of length len(w) bits, such that 1 < b < (w-1), steps 4.1-4.2
      BigInteger b = new BigInteger(w.bitLength(), rnd);
      while (b.compareTo(BigInteger.ONE) <= 0 || b.compareTo(w) >= 0)
      {
        b = new BigInteger(w.bitLength(), rnd);
      }

      // Construct z = b^m mod w, step 4.3
      int j = 0;
      BigInteger z = ModPowAbstraction.modPow(b, m, w);
      while (!((j == 0 && z.equals(BigInteger.ONE)) || z.equals(wMinusOne))) // step 4.4-4.5
      {
        if (j > 0 && z.equals(BigInteger.ONE) || ++j == a)
        {
          return false;
        }
        z = ModPowAbstraction.modPow(z, BigInteger.valueOf(2), w); // step 4.5.1
      }
    }
    return true;
  }
}
