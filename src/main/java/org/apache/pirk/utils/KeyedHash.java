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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Class for the PIR keyed hash
 * <p>
 * Defaults to java hashCode(); can optionally choose MD5, SHA-1, or SHA-256
 * 
 */
public class KeyedHash
{
  private static final Logger logger = LoggerFactory.getLogger(KeyedHash.class);

  /**
   * Hash method that uses the java String hashCode()
   */
  public static int hash(String key, int bitSize, String input)
  {
    int fullHash = (key + input).hashCode();

    // Take only the lower bitSize-many bits of the resultant hash
    int bitLimitedHash = fullHash;
    if (bitSize < 32)
    {
      bitLimitedHash = (0xFFFFFFFF >>> (32 - bitSize)) & fullHash;
    }

    return bitLimitedHash;
  }

  /**
   * Hash method to optionally specify a hash type other than the default java hashCode() hashType must be MD5, SHA-1, or SHA-256
   * 
   */
  public static int hash(String key, int bitSize, String input, String hashType)
  {
    int bitLimitedHash;

    try
    {
      MessageDigest md = MessageDigest.getInstance(hashType);
      byte[] array = md.digest(input.getBytes());

      int hashInt = fromByteArray(array);
      bitLimitedHash = hashInt;
      if (bitSize < 32)
      {
        bitLimitedHash = (0xFFFFFFFF >>> (32 - bitSize)) & hashInt;
      }
      logger.debug("hashInt = " + hashInt + " bitLimitedHash = " + bitLimitedHash);

    } catch (NoSuchAlgorithmException e)
    {

      logger.info(e.toString());
      bitLimitedHash = hash(key, bitSize, input);
    }

    return bitLimitedHash;
  }

  private static int fromByteArray(byte[] bytes)
  {
    return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
  }
}
