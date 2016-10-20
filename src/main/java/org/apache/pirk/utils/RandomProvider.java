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
package org.apache.pirk.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

/**
 * Class that provides access to an existing SecureRandom object.
 * <p>
 * SECURE_RANDOM is a globally available SecureRandom instantiated based on the "pallier.secureRandom.algorithm" and "pallier.secureRandom.provider"
 * configuration variables.
 * <p>
 * This is safe because there is no way for a user to make the quality of the generated random worse nor to reveal critical state of the PRNG.
 * <p>
 * The two methods that would appear to cause problems <i>but don't</i> are:
 * <ul>
 * <li> {@code setSeed} - setSeed doesn't replace the seed in the SecureRandom object but instead "the given seed supplements, rather than replaces, the
 * existing seed. Thus, repeated calls are guaranteed never to reduce randomness".
 * <li> {@code getSeed} - getSeed doesn't return the seed of the SecureRandom object but returns new seed material generated with the same seed generation
 * algorithm used to create the instance.
 * <p>
 * </ul>
 */
public class RandomProvider
{
  public static final SecureRandom SECURE_RANDOM;
  private static final Logger logger = LoggerFactory.getLogger(RandomProvider.class);

  static
  {
    try
    {
      String alg = SystemConfiguration.getProperty("pallier.secureRandom.algorithm");
      if (alg == null)
      {
        SECURE_RANDOM = new SecureRandom();
      }
      else
      {
        String provider = SystemConfiguration.getProperty("pallier.secureRandom.provider");
        SECURE_RANDOM = (provider == null) ? SecureRandom.getInstance(alg) : SecureRandom.getInstance(alg, provider);
      }
      logger.info("Using secure random from " + SECURE_RANDOM.getProvider().getName() + ":" + SECURE_RANDOM.getAlgorithm());
    } catch (GeneralSecurityException e)
    {
      logger.error("Unable to instantiate a SecureRandom object with the requested algorithm.", e);
      throw new RuntimeException("Unable to instantiate a SecureRandom object with the requested algorithm.", e);
    }
  }

  /**
   * Return a globally available SecureRandom instantiated based on the "pallier.secureRandom.algorithm" and "pallier.secureRandom.provider" configuration
   * variables.
   * <p>
   * This is safe because there is no way for a caller to make the quality of the generated random worse nor to reveal critical state of the PRNG.
   * <p>
   * The two methods that would appear to cause problems <i>but don't</i> are:
   * <ul>
   * <li> {@code setSeed} - setSeed doesn't replace the seed in the SecureRandom object but instead "the given seed supplements, rather than replaces, the
   * existing seed. Thus, repeated calls are guaranteed never to reduce randomness".
   * <li> {@code getSeed} - getSeed doesn't return the seed of the SecureRandom object but returns new seed material generated with the same seed generation
   * algorithm used to create the instance.
   * <p>
   * </ul>
   *
   * @return The pre-existing SecureRandom object instantiated based on the "pallier.secureRandom.algorithm" and "pallier.secureRandom.provider"
   * configuration variables.
   */
  public static SecureRandom getSecureRandom()
  {
    return SECURE_RANDOM;
  }
}
