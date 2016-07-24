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
package org.apache.pirk.encryption;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import org.apache.log4j.Logger;
import org.apache.pirk.utils.LogUtils;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;

/**
 * Implementation of the Paillier cryptosystem
 * <p>
 * The algorithm is as follows:
 * <p>
 * Let N=pq, be a RSA modulus where p,q are large primes of roughly the same length
 * <p>
 * The plaintext space is the additive group Z/NZ and the ciphertext space is the multiplicative group (Z/N^2 Z)*.
 * <p>
 * Public key: N, Private key: The factorization of N=pq.
 * <p>
 * Let lambda(N) be the Carmichael function of N (the exponent of the multiplicative group of units modulo N):
 * <p>
 * lambda(N) = lcm(p-1,q-1) = (p-1)(q-1)/gcd(p-1,q-1)
 * <p>
 * Encryption E(m) for a message m is as follows:
 * <p>
 * - Given N and m
 * <p>
 * - Select a random value r in (Z/NZ)*
 * <p>
 * - E(m) = (1 + mN)r^N mod N^2
 * <p>
 * Decryption D(c) for a ciphertext c is as follows:
 * <p>
 * - Given N, its factorization N=pq, and ciphertext c
 * <p>
 * - Set w = lambda(N)^-1 mod N
 * <p>
 * - Set x = c^(lambda(N))mod N^2
 * <p>
 * - Set y = (x-1)/N
 * <p>
 * - D(c) = yw mod N
 * <p>
 * Ref: Paillier, Pascal. "Public-Key Cryptosystems Based on Composite Degree Residuosity Classes." EUROCRYPT'99.
 */
public class Paillier implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static Logger logger = LogUtils.getLoggerForThisClass();

  private static final SecureRandom secureRandom;

  static
  {
    try
    {
      String alg = SystemConfiguration.getProperty("pallier.secureRandom.algorithm");
      if (alg == null)
      {
        secureRandom = new SecureRandom();
      }
      else
      {
        String provider = SystemConfiguration.getProperty("pallier.secureRandom.provider");
        secureRandom = (provider == null) ? SecureRandom.getInstance(alg) : SecureRandom.getInstance(alg, provider);
      }
      logger.info("Using secure random from " + secureRandom.getProvider().getName() + ":" + secureRandom.getAlgorithm());
    } catch (GeneralSecurityException e)
    {
      logger.error("Unable to instantiate a SecureRandom object with the requested algorithm.", e);
      throw new RuntimeException("Unable to instantiate a SecureRandom object with the requested algorithm.", e);
    }
  }

  BigInteger p = null; // large prime
  BigInteger q = null; // large prime
  BigInteger N = null; // N=pq, RSA modulus

  BigInteger NSquared = null; // NSquared = N^2
  BigInteger lambdaN = null; // lambda(N) = lcm(p-1,q-1), Carmichael function of N
  BigInteger w = null; // lambda(N)^-1 mod N

  int bitLength = 0; // bit length of the modulus N

  /**
   * Constructor with all parameters p,q, and bitLengthInput specified
   * <p>
   * Only used, at this point, for testing purposes
   *
   */
  public Paillier(BigInteger pInput, BigInteger qInput, int bitLengthInput) throws PIRException
  {
    bitLength = bitLengthInput;

    // Verify the prime conditions are satisfied
    int primeCertainty = Integer.parseInt(SystemConfiguration.getProperty("pir.primeCertainty", "128"));
    BigInteger three = BigInteger.valueOf(3);
    if ((pInput.compareTo(three) < 0) || (qInput.compareTo(three) < 0) || pInput.equals(qInput) || !pInput.isProbablePrime(primeCertainty)
        || !qInput.isProbablePrime(primeCertainty))
    {
      throw new PIRException("pInput = " + pInput + " qInput = " + qInput + " do not satisfy primality constraints");
    }

    p = pInput;
    q = qInput;

    N = p.multiply(q);

    setDerivativeElements();

    logger.info("Parameters = " + parametersToString());
  }

  /**
   * Constructor to generate keys given the desired bitLength and prime certainty value
   * <p>
   * The probability that the new BigInteger values represents primes will exceed (1 - (1/2)^certainty). The execution time of this constructor is proportional
   * to the value of this parameter.
   * 
   */
  public Paillier(int bitLengthInput, int certainty) throws PIRException
  {
    this(bitLengthInput, certainty, -1);

    logger.info("Parameters = " + parametersToString());
  }

  /**
   * Constructor to generate keys given the desired bitLength and prime certainty value
   * <p>
   * Can optionally, ensure a certain bit is set in the modulus (if ensureBitSet != 0)
   * <p>
   * The probability that the new BigInteger values represents primes will exceed (1 - (1/2)^certainty). The execution time of this constructor is proportional
   * to the value of this parameter.
   * 
   */
  public Paillier(int bitLengthInput, int certainty, int ensureBitSet) throws PIRException
  {
    bitLength = bitLengthInput;

    int systemPrimeCertainty = Integer.parseInt(SystemConfiguration.getProperty("pir.primeCertainty", "128"));
    if (certainty < systemPrimeCertainty)
    {
      throw new PIRException("Input certainty = " + certainty + " is less than allowed system lower bound = " + systemPrimeCertainty);
    }
    if (ensureBitSet >= bitLengthInput)
    {
      throw new PIRException("ensureBitSet = " + ensureBitSet + " must be less than bitLengthInput = " + bitLengthInput);
    }
    generateKeys(certainty, ensureBitSet);
    setDerivativeElements();

    logger.info("Parameters = " + parametersToString());
  }

  /**
   * Copy Constructior
   * 
   */
  public Paillier(BigInteger p, BigInteger q, int bitLength, BigInteger N, BigInteger NSquared, BigInteger lambdaN, BigInteger w)
  {
    this.p = p;
    this.q = q;
    this.bitLength = bitLength;
    this.N = N;
    this.NSquared = NSquared;
    this.lambdaN = lambdaN;
    this.w = w;
  }

  public BigInteger getP()
  {
    return p;
  }

  public BigInteger getQ()
  {
    return q;
  }

  public BigInteger getN()
  {
    return N;
  }

  public BigInteger getNSquared()
  {
    return NSquared;
  }

  public BigInteger getLambdaN()
  {
    return lambdaN;
  }

  public int getBitLength()
  {
    return bitLength;
  }

  private void generateKeys(int certainty, int ensureBitSet)
  {
    if (ensureBitSet != -1)
    {
      while (true)
      {
        getKeys(certainty);
        if (N.testBit(ensureBitSet))
        {
          logger.info("testBit true\n N = " + N.toString(2));
          break;
        }
        else
        {
          logger.info("testBit false\n N = " + N.toString(2));
        }
      }
    }
    else
    {
      getKeys(certainty);
    }
  }

  private void getKeys(int certainty)
  {
    // Generate the primes
    BigInteger[] pq = PrimeGenerator.getPrimePair(bitLength, certainty, secureRandom);
    p = pq[0];
    q = pq[1];

    N = p.multiply(q);
  }

  private void setDerivativeElements()
  {
    NSquared = N.multiply(N);

    // lambda(N) = lcm(p-1,q-1)
    lambdaN = p.subtract(BigInteger.ONE).multiply(q.subtract(BigInteger.ONE)).divide(p.subtract(BigInteger.ONE).gcd(q.subtract(BigInteger.ONE)));

    w = lambdaN.modInverse(N); // lambda(N)^-1 mod N
  }

  /**
   * Encrypt - generate r
   * 
   */
  public BigInteger encrypt(BigInteger m) throws PIRException
  {
    // Generate a random value r in (Z/NZ)*
    BigInteger r = (new BigInteger(bitLength, secureRandom)).mod(N);
    while (r.mod(p).equals(BigInteger.ZERO) || r.mod(q).equals(BigInteger.ZERO) || r.equals(BigInteger.ONE) || r.equals(BigInteger.ZERO))
    {
      r = (new BigInteger(bitLength, secureRandom)).mod(N);
    }

    return encrypt(m, r);
  }

  /**
   * Encrypt - use provided r
   * 
   */
  public BigInteger encrypt(BigInteger m, BigInteger r) throws PIRException
  {
    BigInteger cipher = null;

    if (m.compareTo(N) >= 0)
    {
      throw new PIRException("m  = " + m.toString(2) + " is greater than or equal to N = " + N.toString(2));
    }

    // E(m) = (1 + mN)r^N mod N^2 = (((1+mN) mod N^2) * (r^N mod N^2)) mod N^2
    BigInteger term1 = (m.multiply(N).add(BigInteger.ONE)).mod(NSquared);
    BigInteger term2 = ModPowAbstraction.modPow(r, N, NSquared);

    cipher = (term1.multiply(term2)).mod(NSquared);

    return cipher;
  }

  /**
   * Method to decrypt a given ciphertext
   */
  public BigInteger decrypt(BigInteger c)
  {
    BigInteger d = null;

    // w = lambda(N)^-1 mod N; x = c^(lambda(N)) mod N^2; y = (x-1)/N; d = yw mod N
    BigInteger x = ModPowAbstraction.modPow(c, lambdaN, NSquared);
    BigInteger y = (x.subtract(BigInteger.ONE)).divide(N);

    d = (y.multiply(w)).mod(N);

    return d;
  }

  private String parametersToString()
  {
    String paramsString = null;

    paramsString = "p = " + p.intValue() + " q = " + q.intValue() + " N = " + N.intValue() + " NSquared = " + NSquared.intValue() + " lambdaN = "
        + lambdaN.intValue() + " bitLength = " + bitLength;

    return paramsString;
  }

  public Paillier copy()
  {
    Paillier paillierCopy = new Paillier(p, q, bitLength, N, NSquared, lambdaN, w);

    return paillierCopy;
  }
}
