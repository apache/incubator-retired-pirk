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
package test.general;

import static org.junit.Assert.assertEquals;

import org.apache.pirk.utils.KeyedHash;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic functional tests for KeyedHash
 * 
 */
public class KeyedHashTest
{
  private static final Logger logger = LoggerFactory.getLogger(KeyedHashTest.class);

  @Test
  public void testKeyedHash()
  {
    logger.info("Starting testKeyedHash: ");

    int hash1 = KeyedHash.hash("someKey", 12, "someInput");
    logger.info("hash1 = " + hash1 + " hash1 = " + Integer.toString(hash1, 2));

    int hash2 = KeyedHash.hash("someKey", 32, "someInput");
    logger.info("hash2 = " + hash2 + " hash2 = " + Integer.toString(hash2, 2));

    int hash3 = KeyedHash.hash("someKey", 34, "someInput");
    logger.info("hash3 = " + hash3 + " hash3 = " + Integer.toString(hash3, 2));

    assertEquals(hash2, hash3);
    assertEquals(hash1, hash2 & 0xFFF);

    logger.info("Successfully completed testKeyedHash");
  }

  @Test
  public void testKeyedHashWithType()
  {
    testKeyedHashType("MD5");
    testKeyedHashType("SHA-1");
    testKeyedHashType("SHA-256");
    testKeyedHashType("FAKE-HASH-TYPE");
  }

  private void testKeyedHashType(String type)
  {
    logger.info("Starting testKeyedHashType with type: " + type);

    int hash1 = KeyedHash.hash("someKey", 12, "someInput", type);
    logger.info("hash1 = " + hash1 + " hash1 = " + Integer.toString(hash1, 2));

    int hash2 = KeyedHash.hash("someKey", 32, "someInput", type);
    logger.info("hash2 = " + hash2 + " hash2 = " + Integer.toString(hash2, 2));

    int hash3 = KeyedHash.hash("someKey", 34, "someInput", type);
    logger.info("hash3 = " + hash3 + " hash3 = " + Integer.toString(hash3, 2));

    assertEquals(hash2, hash3);
    assertEquals(hash1, hash2 & 0xFFF);

    logger.info("Successfully completed testKeyedHashType with type: " + type);
  }
}
