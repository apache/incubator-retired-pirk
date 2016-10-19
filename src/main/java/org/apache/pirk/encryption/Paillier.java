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

import static org.apache.pirk.utils.RandomProvider.getSecureRandom;

import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * Implementation of the Paillier cryptosystem.
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
public final class Paillier implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(Paillier.class);

  private BigInteger p; // large prime
  private BigInteger q; // large prime
  private BigInteger N; // N=pq, RSA modulus

  private BigInteger NSquared; // NSquared = N^2
  private BigInteger lambdaN; // lambda(N) = lcm(p-1,q-1), Carmichael function of N
  private BigInteger w; // lambda(N)^-1 mod N

  private final int bitLength; // bit length of the modulus N

  /**
   * Creates a Paillier algorithm with all parameters specified.
   *
   * @param p
   *          First large prime.
   * @param q
   *          Second large prime.
   * @param bitLength
   *          Bit length of the modulus {@code N}.
   * @throws IllegalArgumentException
   *           If {@code p} or {@code q} do not satisfy primality constraints.
   */
  public Paillier(BigInteger p, BigInteger q, int bitLength)
  {
    this.bitLength = bitLength;

    // Verify the prime conditions are satisfied
    int primeCertainty = SystemConfiguration.getIntProperty("pir.primeCertainty", 128);
    BigInteger three = BigInteger.valueOf(3);
    if ((p.compareTo(three) < 0) || (q.compareTo(three) < 0) || p.equals(q) || !p.isProbablePrime(primeCertainty) || !q.isProbablePrime(primeCertainty))
    {
      throw new IllegalArgumentException("p = " + p + " q = " + q + " do not satisfy primality constraints");
    }

    this.p = p;
    this.q = q;

    this.N = p.multiply(q);

    setDerivativeElements();

    logger.info("Parameters = " + parametersToString());
  }

  /**
   * Constructs a Paillier algorithm with generated keys.
   * <p>
   * The generated keys {@code p} and {@code q} will have half the given modulus bit length, and the given prime certainty.
   * <p>
   * The probability that the generated keys represent primes will exceed (1 - (1/2)<sup>{@code certainty}</sup>). The execution time of this constructor is
   * proportional to the value of this parameter.
   *
   * @param bitLength
   *          The bit length of the resulting modulus {@code N}.
   * @param certainty
   *          The probability that the new {@code p} and {@code q} represent prime numbers.
   * @throws IllegalArgumentException
   *           If the {@code certainty} is less than the system allowed lower bound.
   */
  public Paillier(int bitLength, int certainty)
  {
    this(bitLength, certainty, -1);
  }

  /**
   * Constructs a Paillier algorithm with generated keys and optionally ensures a certain bit is set in the modulus.
   * <p>
   * The generated keys {@code p} and {@code q} will have half the given modulus bit length, and the given prime certainty.
   * <p>
   * The probability that the generated keys represent primes will exceed (1 - (1/2)<sup>{@code certainty}</sup>). The execution time of this constructor is
   * proportional to the value of this parameter.
   * <p>
   * When ensureBitSet > -1 the value of bit "{@code ensureBitSet}" in modulus {@code N} will be set.
   *
   * @param bitLength
   *          The bit length of the resulting modulus {@code N}.
   * @param certainty
   *          The probability that the new {@code p} and {@code q} represent prime numbers.
   * @param ensureBitSet
   *          index of bit in {@code N} to ensure is set.
   * @throws IllegalArgumentException
   *           If the {@code certainty} is less than the system allowed lower bound, or the index of {@code ensureBitSet} is greater than the {@code bitLength}.
   */
  public Paillier(int bitLength, int certainty, int ensureBitSet)
  {
    int systemPrimeCertainty = SystemConfiguration.getIntProperty("pir.primeCertainty", 128);
    if (certainty < systemPrimeCertainty)
    {
      throw new IllegalArgumentException("Input certainty = " + certainty + " is less than allowed system lower bound = " + systemPrimeCertainty);
    }
    if (ensureBitSet >= bitLength)
    {
      throw new IllegalArgumentException("ensureBitSet = " + ensureBitSet + " must be less than bitLengthInput = " + bitLength);
    }
    this.bitLength = bitLength;
    generateKeys(bitLength, certainty, ensureBitSet);
    setDerivativeElements();

    logger.info("Parameters = " + parametersToString());
  }

  /**
   * Returns the value of the large prime {@code p}.
   *
   * @return p.
   */
  public BigInteger getP()
  {
    return p;
  }

  /**
   * Returns the value of the large prime {@code q}.
   *
   * @return q.
   */
  public BigInteger getQ()
  {
    return q;
  }

  /**
   * Returns the RSA modulus value {@code N}.
   *
   * @return N, the product of {@code p} and {@code q}.
   */
  public BigInteger getN()
  {
    return N;
  }

  /**
   * Returns the value of {@code N}<sup>2</sup>.
   *
   * @return N squared.
   */
  public BigInteger getNSquared()
  {
    return NSquared;
  }

  /**
   * Returns the value of Carmichael's function at {@code N}.
   * <p>
   * The Carmichael function of {@code N} is the least common multiple of {@code p-1} and {@code q-1},
   *
   * @return Carmichael's function at {@code N}.
   */
  public BigInteger getLambdaN()
  {
    return lambdaN;
  }

  /**
   * Returns the bit length of the modulus {@code N}.
   *
   * @return the bit length, as an integer.
   */
  public int getBitLength()
  {
    return bitLength;
  }

  private void generateKeys(int bitLength, int certainty, final int ensureBitSet)
  {
    getKeys(bitLength, certainty);

    if (ensureBitSet > -1)
    {
      while (!N.testBit(ensureBitSet))
      {
        logger.info("testBit false\n N = " + N.toString(2));
        getKeys(bitLength, certainty);
      }
      logger.info("testBit true\n N = " + N.toString(2));
    }
  }

  private void getKeys(int bitLength, int certainty)
  {
    // Generate the primes
    BigInteger[] pq = PrimeGenerator.getPrimePair(bitLength, certainty, getSecureRandom());
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
   * Returns the encrypted value of {@code m} using a generated random value.
   *
   * @param m
   *          the value to be encrypted.
   * @return the encrypted value
   * @throws PIRException
   *           If {@code m} is not less than @{code N}.
   */
  public BigInteger encrypt(BigInteger m) throws PIRException
  {
    // Generate a random value r in (Z/NZ)*
    BigInteger r = (new BigInteger(bitLength, getSecureRandom())).mod(N);
    while (r.equals(BigInteger.ZERO) || r.equals(BigInteger.ONE) || r.mod(p).equals(BigInteger.ZERO) || r.mod(q).equals(BigInteger.ZERO))
    {
      r = (new BigInteger(bitLength, getSecureRandom())).mod(N);
    }

    return encrypt(m, r);
  }

  /**
   * Returns the ciphertext of a message using the given random value.
   *
   * @param m
   *          the value to be encrypted.
   * @param r
   *          the random value to use in the Pailler encryption.
   * @return the encrypted value.
   * @throws PIRException
   *           If {@code m} is not less than @{code N}.
   */
  public BigInteger encrypt(BigInteger m, BigInteger r) throws PIRException
  {
    if (m.compareTo(N) >= 0)
    {
      throw new PIRException("m  = " + m.toString(2) + " is greater than or equal to N = " + N.toString(2));
    }

    // E(m) = (1 + mN)r^N mod N^2 = (((1+mN) mod N^2) * (r^N mod N^2)) mod N^2
    BigInteger term1 = (m.multiply(N).add(BigInteger.ONE)).mod(NSquared);
    BigInteger term2 = ModPowAbstraction.modPow(r, N, NSquared);

    return (term1.multiply(term2)).mod(NSquared);
  }

  /**
   * Returns the plaintext message for a given ciphertext.
   *
   * @param c
   *          an encrypted value.
   * @return the corresponding plaintext value.
   */
  public BigInteger decrypt(BigInteger c)
  {
    // w = lambda(N)^-1 mod N; x = c^(lambda(N)) mod N^2; y = (x-1)/N; d = yw mod N
    BigInteger x = ModPowAbstraction.modPow(c, lambdaN, NSquared);
    BigInteger y = (x.subtract(BigInteger.ONE)).divide(N);

    return (y.multiply(w)).mod(N);
  }

  private String parametersToString()
  {
    return "p = " + p.intValue() + " q = " + q.intValue() + " N = " + N.intValue() + " NSquared = " + NSquared.intValue() + " lambdaN = " + lambdaN.intValue()
        + " bitLength = " + bitLength;
  }
}
