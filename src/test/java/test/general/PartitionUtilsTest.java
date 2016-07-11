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
package test.general;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.pirk.schema.data.partitioner.IPDataPartitioner;
import org.apache.pirk.schema.data.partitioner.ISO8601DatePartitioner;
import org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner;
import org.apache.pirk.utils.LogUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.junit.Test;

/**
 * Class to functionally test the bit conversion utils
 */
public class PartitionUtilsTest
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  PrimitiveTypePartitioner primitivePartitioner = null;

  public PartitionUtilsTest()
  {
    primitivePartitioner = new PrimitiveTypePartitioner();
  }

  @Test
  public void testMask()
  {
    logger.info("Starting testMask: ");

    BigInteger mask = PrimitiveTypePartitioner.formBitMask(4); // 1111

    assertEquals(mask.intValue(), 15);

    logger.info("Successfully completed testMask");
  }

  @Test
  public void testPartitionBits()
  {
    logger.info("Starting testPartitionBits: ");

    BigInteger value = new BigInteger("245"); // 11110101
    BigInteger value2 = new BigInteger("983"); // 1111010111

    BigInteger mask4 = PrimitiveTypePartitioner.formBitMask(4); // 1111
    BigInteger mask8 = PrimitiveTypePartitioner.formBitMask(8); // 11111111

    try
    {
      ArrayList<BigInteger> partitions = PrimitiveTypePartitioner.partitionBits(value, 4, mask4);

      assertEquals(2, partitions.size());
      assertEquals(partitions.get(0).intValue(), 15); // 1111
      assertEquals(partitions.get(1).intValue(), 5); // 0101

    } catch (Exception e)
    {
      fail(e.toString());
    }

    try
    {
      ArrayList<BigInteger> partitions = PrimitiveTypePartitioner.partitionBits(value2, 4, mask4);

      assertEquals(3, partitions.size());
      assertEquals(partitions.get(0).intValue(), 15); // 1111
      assertEquals(partitions.get(1).intValue(), 5); // 0101
      assertEquals(partitions.get(2).intValue(), 3); // 11

    } catch (Exception e)
    {
      fail(e.toString());
    }
    try
    {
      ArrayList<BigInteger> partitions = PrimitiveTypePartitioner.partitionBits(value, 8, mask8);

      assertEquals(1, partitions.size());
      assertEquals(partitions.get(0).intValue(), 245);

    } catch (Exception e)
    {
      fail(e.toString());
    }

    try
    {
      ArrayList<BigInteger> partitions = PrimitiveTypePartitioner.partitionBits(value, 4, mask8);

      fail("BitConversionUtils.partitionBits did not throw error for mismatched partitionSize and mask size");

    } catch (Exception e)
    {}

    logger.info("Successfully completed testPartitionBits");
  }

  @Test
  public void testPartitions() throws Exception
  {
    logger.info("Starting testToPartitions:");

    PrimitiveTypePartitioner primitivePartitioner = new PrimitiveTypePartitioner();
    IPDataPartitioner ipPartitioner = new IPDataPartitioner();
    ISO8601DatePartitioner datePartitioner = new ISO8601DatePartitioner();

    // Test IP
    String ipTest = "127.0.0.1";
    ArrayList<BigInteger> partsIP = ipPartitioner.toPartitions(ipTest, PrimitiveTypePartitioner.STRING);
    assertEquals(4, partsIP.size());
    assertEquals(ipTest, ipPartitioner.fromPartitions(partsIP, 0, PrimitiveTypePartitioner.STRING));

    // Test Date
    String dateTest = "2016-02-20T23:29:05.000Z";
    ArrayList<BigInteger> partsDate = datePartitioner.toPartitions(dateTest, null);
    assertEquals(8, partsDate.size());
    assertEquals(dateTest, datePartitioner.fromPartitions(partsDate, 0, null));

    // Test byte
    byte bTest = Byte.parseByte("10");
    ArrayList<BigInteger> partsByte = primitivePartitioner.toPartitions(bTest, PrimitiveTypePartitioner.BYTE);
    assertEquals(1, partsByte.size());
    assertEquals(bTest, primitivePartitioner.fromPartitions(partsByte, 0, PrimitiveTypePartitioner.BYTE));

    ArrayList<BigInteger> partsByteMax = primitivePartitioner.toPartitions(Byte.MAX_VALUE, PrimitiveTypePartitioner.BYTE);
    assertEquals(1, partsByteMax.size());
    assertEquals(Byte.MAX_VALUE, primitivePartitioner.fromPartitions(partsByteMax, 0, PrimitiveTypePartitioner.BYTE));

    // Test string
    String stringBits = SystemConfiguration.getProperty("pir.stringBits");
    SystemConfiguration.setProperty("pir.stringBits", "64");
    testString("testString"); // over the allowed bit size
    testString("t"); // under the allowed bit size
    SystemConfiguration.setProperty("pir.stringBits", stringBits);

    // Test short
    short shortTest = new Short("2456").shortValue();
    ArrayList<BigInteger> partsShort = primitivePartitioner.toPartitions(shortTest, PrimitiveTypePartitioner.SHORT);
    assertEquals(2, partsShort.size());
    assertEquals(shortTest, primitivePartitioner.fromPartitions(partsShort, 0, PrimitiveTypePartitioner.SHORT));

    ArrayList<BigInteger> partsShortMax = primitivePartitioner.toPartitions(Short.MAX_VALUE, PrimitiveTypePartitioner.SHORT);
    assertEquals(2, partsShortMax.size());
    assertEquals(Short.MAX_VALUE, primitivePartitioner.fromPartitions(partsShortMax, 0, PrimitiveTypePartitioner.SHORT));

    // Test int
    int intTest = Integer.parseInt("-5789");
    ArrayList<BigInteger> partsInt = primitivePartitioner.toPartitions(intTest, PrimitiveTypePartitioner.INT);
    assertEquals(4, partsInt.size());
    assertEquals(intTest, primitivePartitioner.fromPartitions(partsInt, 0, PrimitiveTypePartitioner.INT));

    ArrayList<BigInteger> partsIntMax = primitivePartitioner.toPartitions(Integer.MAX_VALUE, PrimitiveTypePartitioner.INT);
    assertEquals(4, partsIntMax.size());
    assertEquals(Integer.MAX_VALUE, primitivePartitioner.fromPartitions(partsIntMax, 0, PrimitiveTypePartitioner.INT));

    // Test long
    long longTest = Long.parseLong("56789");
    ArrayList<BigInteger> partsLong = primitivePartitioner.toPartitions(longTest, PrimitiveTypePartitioner.LONG);
    assertEquals(8, partsLong.size());
    assertEquals(longTest, primitivePartitioner.fromPartitions(partsLong, 0, PrimitiveTypePartitioner.LONG));

    ArrayList<BigInteger> partsLongMax = primitivePartitioner.toPartitions(Long.MAX_VALUE, PrimitiveTypePartitioner.LONG);
    assertEquals(8, partsLongMax.size());
    assertEquals(Long.MAX_VALUE, primitivePartitioner.fromPartitions(partsLongMax, 0, PrimitiveTypePartitioner.LONG));

    // Test float
    float floatTest = Float.parseFloat("567.77");
    ArrayList<BigInteger> partsFloat = primitivePartitioner.toPartitions(floatTest, PrimitiveTypePartitioner.FLOAT);
    assertEquals(4, partsFloat.size());
    assertEquals(floatTest, primitivePartitioner.fromPartitions(partsFloat, 0, PrimitiveTypePartitioner.FLOAT));

    ArrayList<BigInteger> partsFloatMax = primitivePartitioner.toPartitions(Float.MAX_VALUE, PrimitiveTypePartitioner.FLOAT);
    assertEquals(4, partsFloatMax.size());
    assertEquals(Float.MAX_VALUE, primitivePartitioner.fromPartitions(partsFloatMax, 0, PrimitiveTypePartitioner.FLOAT));

    // Test double
    double doubleTest = Double.parseDouble("567.77");
    ArrayList<BigInteger> partsDouble = primitivePartitioner.toPartitions(doubleTest, PrimitiveTypePartitioner.DOUBLE);
    assertEquals(8, partsDouble.size());
    assertEquals(doubleTest, primitivePartitioner.fromPartitions(partsDouble, 0, PrimitiveTypePartitioner.DOUBLE));

    ArrayList<BigInteger> partsDoubleMax = primitivePartitioner.toPartitions(Double.MAX_VALUE, PrimitiveTypePartitioner.DOUBLE);
    assertEquals(8, partsDoubleMax.size());
    assertEquals(Double.MAX_VALUE, primitivePartitioner.fromPartitions(partsDoubleMax, 0, PrimitiveTypePartitioner.DOUBLE));

    // Test char
    char charTest = 'b';
    ArrayList<BigInteger> partsChar = primitivePartitioner.toPartitions(charTest, PrimitiveTypePartitioner.CHAR);
    assertEquals(2, partsChar.size());
    assertEquals(charTest, primitivePartitioner.fromPartitions(partsChar, 0, PrimitiveTypePartitioner.CHAR));

    ArrayList<BigInteger> partsCharMax = primitivePartitioner.toPartitions(Character.MAX_VALUE, PrimitiveTypePartitioner.CHAR);
    assertEquals(2, partsCharMax.size());
    assertEquals(Character.MAX_VALUE, primitivePartitioner.fromPartitions(partsCharMax, 0, PrimitiveTypePartitioner.CHAR));

    logger.info("Sucessfully completed testToPartitions:");
  }

  private void testString(String testString) throws Exception
  {
    PrimitiveTypePartitioner ptp = new PrimitiveTypePartitioner();

    ArrayList<BigInteger> partsString = ptp.toPartitions(testString, PrimitiveTypePartitioner.STRING);
    int numParts = Integer.parseInt(SystemConfiguration.getProperty("pir.stringBits")) / 8;
    assertEquals(numParts, partsString.size());

    logger.info("testString.getBytes().length = " + testString.getBytes().length);
    int offset = numParts;
    if (testString.getBytes().length < numParts)
    {
      offset = testString.getBytes().length;
    }
    String element = new String(testString.getBytes(), 0, offset);
    assertEquals(element, ptp.fromPartitions(partsString, 0, PrimitiveTypePartitioner.STRING));
  }
}
