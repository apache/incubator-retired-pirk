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

import java.math.BigInteger;

import org.apache.pirk.utils.SystemConfiguration;

import com.squareup.jnagmp.Gmp;

/**
 * This class is designed to offer a one-stop-shop for invoking the desired version of
 * modPow, modularMultiply, and modInverse
 */
public final class IntegerMathAbstraction
{

  private static boolean useGMPForModPow, useGMPConstantTimeMethods, useGMPmodularMultiply, useGMPmodularInverse;

  static
  {
    // Load the configuration
    reloadConfiguration();
  }

  /**
   * Reloads the configuration options for this class. They control which implementations are used for integer
   * math: GMP or BigInteger.
   */
  public static void reloadConfiguration()
  {
    useGMPForModPow = SystemConfiguration.getProperty("paillier.useGMPForModPow").equals("true");
    useGMPConstantTimeMethods = SystemConfiguration.getProperty("paillier.GMPConstantTimeMode").equals("true");
    useGMPmodularMultiply = SystemConfiguration.getProperty("paillier.useGMPForModularMultiply").equals("true");
    useGMPmodularInverse = SystemConfiguration.getProperty("paillier.useGMPForModularInverse").equals("true");
  }

  /**
   * Performs modPow: ({@code base}^{@code exponent}) mod {@code modulus}
   * <p>
   * This method uses the values of {@code paillier.useGMPForModPow} and {@code paillier.GMPConstantTimeMode}
   * as they were when the class was loaded to decide which implementation of modPow to invoke.
   * <p>
   * These values can be reloaded by invoking static method {@code IntegerMathAbstraction.reloadConfiguration()}
   *
   * @return The result of modPow
   */
  public static BigInteger modPow(BigInteger base, BigInteger exponent, BigInteger modulus)
  {
    BigInteger result;

    if (useGMPForModPow)
    {
      if (useGMPConstantTimeMethods)
      {
        // Use GMP and use the "timing attack resistant" method
        // The timing attack resistance slows down performance and is not necessarily proven to block timing attacks.
        // Before getting concerned, please carefully consider your threat model
        // and if you really believe that you may need it
        result = Gmp.modPowSecure(base, exponent, modulus);
      }
      else
      {
        // The word "insecure" here does not imply any actual, direct insecurity.
        // It denotes that this function runs as fast as possible without trying to
        // counteract timing attacks. This is probably what you want unless you have a
        // compelling reason why you believe that this environment is safe enough to house
        // your keys but doesn't protect you from another entity on the machine watching
        // how long the program runs.
        result = Gmp.modPowInsecure(base, exponent, modulus);
      }
    }
    else
    {
      // If GMP isn't used, BigInteger's built-in modPow is used.
      // This is significantly slower but has the virtue of working everywhere.
      result = base.modPow(exponent, modulus);
    }

    return result;
  }

  public static BigInteger modPow(long base, BigInteger exponent, BigInteger modulus)
  {
    return modPow(BigInteger.valueOf(base), exponent, modulus);
  }

  /**
   * Performs modular multiply: ({@code factor1}*{@code factor2}) mod {@code modulus}
   * <p>
   * This method uses the value of {@code paillier.useGMPForModularMultiply} as it was set
   * when the class was loaded to decide which implementation of modular multiplication to invoke.
   * <p>
   * These values can be reloaded by invoking static method {@code IntegerMathAbstraction.reloadConfiguration()}
   *
   * @param factor1 the first factor to the multiplication
   * @param factor2 the second factor to the multiplication
   * @param modulus the modulus to be applied to ({@code factor1}*{@code factor2})
   * @return ({@code factor1}*{@code factor2}) mod {@code modulus}
   */
  public static BigInteger modularMultiply(BigInteger factor1, BigInteger factor2, BigInteger modulus)
  {
    BigInteger result;

    if (useGMPmodularMultiply)
    {
      result = Gmp.modularMultiply(factor1, factor2, modulus);
    }
    else
    {
      result = factor1.multiply(factor2).mod(modulus);
    }

    return result;
  }

  public static BigInteger modInverse(BigInteger dividend, BigInteger modulus)
  {
    BigInteger result;

    if (useGMPmodularInverse)
    {
      result = Gmp.modInverse(dividend, modulus);
    }
    else
    {
      result = dividend.modInverse(modulus);
    }

    return result;
  }
}
