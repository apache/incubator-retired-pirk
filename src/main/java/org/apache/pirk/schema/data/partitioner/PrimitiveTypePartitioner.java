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
package org.apache.pirk.schema.data.partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.util.ByteArrayBuffer;
import org.apache.log4j.Logger;
import org.apache.pirk.utils.LogUtils;
import org.apache.pirk.utils.SystemConfiguration;

/**
 * Class for partitioning objects with primitive Java types
 * 
 */
public class PrimitiveTypePartitioner implements DataPartitioner
{
  private static final long serialVersionUID = 1L;

  private Logger logger = LogUtils.getLoggerForThisClass();

  public final BigInteger bitMask8 = formBitMask(8);

  public static final String BYTE = "byte";
  public static final String SHORT = "short";
  public static final String INT = "int";
  public static final String LONG = "long";
  public static final String FLOAT = "float";
  public static final String DOUBLE = "double";
  public static final String CHAR = "char";
  public static final String STRING = "string";

  // Default constructor
  public PrimitiveTypePartitioner()
  {}

  /**
   * Splits the given BigInteger into partitions given by the partitionSize
   *
   */
  public static ArrayList<BigInteger> partitionBits(BigInteger value, int partitionSize, BigInteger mask) throws Exception
  {
    if (mask.bitLength() != partitionSize)
    {
      throw new Exception("mask.bitLength() " + mask.bitLength() + " != partitionSize = " + partitionSize);
    }

    ArrayList<BigInteger> partitions = new ArrayList<BigInteger>();
    if (value.bitLength() < partitionSize)
    {
      partitions.add(value);
    }
    else
    {
      int bitLength = value.bitLength();
      mask = mask.shiftLeft(bitLength - partitionSize); // shift left for big endian partitioning

      BigInteger result = BigInteger.valueOf(0);
      int partNum = 0;
      for (int i = 0; i < bitLength; i += partitionSize)
      {
        result = value.and(mask);

        int shiftSize = bitLength - (partNum + 1) * partitionSize;
        if (shiftSize < 0) // partitionSize does not divide bitLength, the remaining bits do not need shifting
        {
          shiftSize = 0;
        }

        result = result.shiftRight(shiftSize);
        mask = mask.shiftRight(partitionSize);

        partitions.add(result);
        ++partNum;
      }
    }
    return partitions;
  }

  /**
   * Method to form a BigInteger bit mask for the given partitionSize
   * 
   */
  public static BigInteger formBitMask(int partitionSize)
  {
    BigInteger mask = (BigInteger.valueOf(2).pow(partitionSize)).subtract(BigInteger.ONE);

    return mask;
  }

  /**
   * Method to get the number of 8-bit partitions given the element type
   * 
   */
  @Override
  public int getNumPartitions(String type) throws Exception
  {
    int partitionSize = 8;

    int numParts = 0;
    if (type.equals(BYTE))
    {
      numParts = Byte.SIZE / partitionSize;
    }
    else if (type.equals(SHORT))
    {
      numParts = Short.SIZE / partitionSize;
    }
    else if (type.equals(INT))
    {
      numParts = Integer.SIZE / partitionSize;
    }
    else if (type.equals(LONG))
    {
      numParts = Long.SIZE / partitionSize;
    }
    else if (type.equals(FLOAT))
    {
      numParts = Float.SIZE / partitionSize;
    }
    else if (type.equals(DOUBLE))
    {
      numParts = Double.SIZE / partitionSize;
    }
    else if (type.equals(CHAR))
    {
      numParts = Character.SIZE / partitionSize;
    }
    else if (type.equals(STRING))
    {
      numParts = Integer.parseInt(SystemConfiguration.getProperty("pir.stringBits")) / partitionSize;
    }
    else
    {
      throw new Exception("type = " + type + " not recognized!");
    }
    return numParts;
  }

  /**
   * Get the bit size of the allowed primitive java types
   */
  @Override
  public int getBits(String type) throws Exception
  {
    int bits = 0;
    if (type.equals(BYTE))
    {
      bits = Byte.SIZE;
    }
    else if (type.equals(SHORT))
    {
      bits = Short.SIZE;
    }
    else if (type.equals(INT))
    {
      bits = Integer.SIZE;
    }
    else if (type.equals(LONG))
    {
      bits = Long.SIZE;
    }
    else if (type.equals(FLOAT))
    {
      bits = Float.SIZE;
    }
    else if (type.equals(DOUBLE))
    {
      bits = Double.SIZE;
    }
    else if (type.equals(CHAR))
    {
      bits = Character.SIZE;
    }
    else if (type.equals(STRING))
    {
      bits = Integer.parseInt(SystemConfiguration.getProperty("pir.stringBits"));
    }
    else
    {
      throw new Exception("type = " + type + " not recognized!");
    }
    return bits;
  }

  /**
   * Reconstructs the object from the partitions
   */
  @Override
  public Object fromPartitions(ArrayList<BigInteger> parts, int partsIndex, String type) throws Exception
  {
    Object element = null;

    if (type.equals(BYTE))
    {
      BigInteger bInt = parts.get(partsIndex);
      element = new Byte(bInt.toString()).byteValue();
    }
    else if (type.equals(SHORT))
    {
      byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
      element = ByteBuffer.wrap(bytes).getShort();
    }
    else if (type.equals(INT))
    {
      byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
      element = ByteBuffer.wrap(bytes).getInt();
    }
    else if (type.equals(LONG))
    {
      byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
      element = ByteBuffer.wrap(bytes).getLong();
    }
    else if (type.equals(FLOAT))
    {
      byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
      element = ByteBuffer.wrap(bytes).getFloat();
    }
    else if (type.equals(DOUBLE))
    {
      byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
      element = ByteBuffer.wrap(bytes).getDouble();
    }
    else if (type.equals(CHAR))
    {
      byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
      element = ByteBuffer.wrap(bytes).getChar();
    }
    else if (type.equals(STRING))
    {
      byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
      element = new String(bytes).trim(); // this should remove 0 padding added for partitioning underflowing strings
    }
    else
    {
      throw new Exception("type = " + type + " not recognized!");
    }
    return element;
  }

  public byte[] appendBytes(ArrayList<BigInteger> parts, int partsIndex, int numParts)
  {
    ByteArrayBuffer buf = new ByteArrayBuffer(numParts);
    for (int i = 0; i < numParts; ++i)
    {
      byte partByte = parts.get(partsIndex + i).byteValue();
      buf.append(partByte);
    }

    return buf.buffer();
  }

  /**
   * 
   * Partitions an object to an ArrayList of BigInteger values, currently represents an 8-bit partitioning
   */
  @Override
  public ArrayList<BigInteger> toPartitions(Object obj, String type) throws Exception
  {
    ArrayList<BigInteger> parts = new ArrayList<BigInteger>();

    int numParts = getNumPartitions(type);
    if (type.equals(BYTE))
    {
      if (obj instanceof String)
      {
        parts.add(new BigInteger(ByteBuffer.allocate(1).put(Byte.parseByte((String) obj)).array()));
      }
      else
      {
        parts.add(new BigInteger(ByteBuffer.allocate(1).put((byte) obj).array()));
      }
    }
    else if (type.equals(STRING))
    {
      byte[] stringBytes = ((String) obj).getBytes();
      for (int i = 0; i < numParts; ++i)
      {
        if (i < stringBytes.length)
        {
          parts.add(new BigInteger(ByteBuffer.allocate(1).put(stringBytes[i]).array()));
        }
        else
        {
          parts.add(new BigInteger(ByteBuffer.allocate(1).put(Byte.parseByte("0")).array()));
        }
      }
    }
    else
    {
      // Extract the byte array
      byte[] bytes = null;
      if (type.equals(SHORT))
      {
        if (obj instanceof String)
        {
          bytes = ByteBuffer.allocate(numParts).putShort(Short.parseShort((String) obj)).array();
        }
        else
        {
          bytes = ByteBuffer.allocate(numParts).putShort((short) obj).array();
        }
      }
      else if (type.equals(INT))
      {
        if (obj instanceof String)
        {
          bytes = ByteBuffer.allocate(numParts).putInt(Integer.parseInt((String) obj)).array();
        }
        else
        {
          bytes = ByteBuffer.allocate(numParts).putInt((int) obj).array();
        }
      }
      else if (type.equals(LONG))
      {
        if (obj instanceof String)
        {
          bytes = ByteBuffer.allocate(numParts).putLong(Long.parseLong((String) obj)).array();
        }
        else
        {
          bytes = ByteBuffer.allocate(numParts).putLong((long) obj).array();
        }
      }
      else if (type.equals(FLOAT))
      {
        if (obj instanceof String)
        {
          bytes = ByteBuffer.allocate(numParts).putFloat(Float.parseFloat((String) obj)).array();
        }
        else
        {
          bytes = ByteBuffer.allocate(numParts).putFloat((float) obj).array();
        }
      }
      else if (type.equals(DOUBLE))
      {
        if (obj instanceof String)
        {
          bytes = ByteBuffer.allocate(numParts).putDouble(Double.parseDouble((String) obj)).array();
        }
        else
        {
          bytes = ByteBuffer.allocate(numParts).putDouble((double) obj).array();
        }
      }
      else if (type.equals(CHAR))
      {
        if (obj instanceof String)
        {
          bytes = ByteBuffer.allocate(numParts).putChar(((String) (obj)).charAt(0)).array();
        }
        else
        {
          bytes = ByteBuffer.allocate(numParts).putChar((char) obj).array();
        }
      }

      // Add bytes to parts ArrayList
      for (byte b : bytes)
      {
        // Make sure that BigInteger treats the byte as 'unsigned' literal
        parts.add(BigInteger.valueOf(((long) b) & 0xFF));
      }
    }

    return parts;
  }

  /**
   * Method to get an empty set of partitions by data type - used for padding return array values
   */
  @Override
  public ArrayList<BigInteger> getPaddedPartitions(String type) throws Exception
  {
    ArrayList<BigInteger> parts = new ArrayList<BigInteger>();

    int numParts = getNumPartitions(type);
    if (type.equals(BYTE))
    {
      parts.add(new BigInteger(ByteBuffer.allocate(1).put(Byte.parseByte("0")).array()));
    }
    else if (type.equals(STRING))
    {
      for (int i = 0; i < numParts; ++i)
      {
        parts.add(new BigInteger(ByteBuffer.allocate(1).put(Byte.parseByte("0")).array()));
      }
    }
    else
    {
      // Extract the byte array
      byte[] bytes = null;
      if (type.equals(SHORT))
      {
        bytes = ByteBuffer.allocate(numParts).putShort(Short.parseShort("0")).array();
      }
      else if (type.equals(INT))
      {
        bytes = ByteBuffer.allocate(numParts).putInt(Integer.parseInt("0")).array();
      }
      else if (type.equals(LONG))
      {
        bytes = ByteBuffer.allocate(numParts).putLong(Long.parseLong("0")).array();
      }
      else if (type.equals(FLOAT))
      {
        bytes = ByteBuffer.allocate(numParts).putFloat(Float.parseFloat("0")).array();
      }
      else if (type.equals(DOUBLE))
      {
        bytes = ByteBuffer.allocate(numParts).putDouble(Double.parseDouble("0")).array();
      }
      else if (type.equals(CHAR))
      {
        bytes = ByteBuffer.allocate(numParts).putChar('0').array();
      }

      // Add bytes to parts ArrayList
      for (byte b : bytes)
      {
        parts.add(new BigInteger(ByteBuffer.allocate(1).put(b).array()));
      }
    }
    return parts;
  }

  /**
   * Create partitions for an array of the same type of elements - used when a data value field is an array and we wish to encode these into the return value
   */
  @Override
  public ArrayList<BigInteger> arrayToPartitions(List<?> elementList, String type) throws Exception
  {
    ArrayList<BigInteger> parts = new ArrayList<BigInteger>();

    int numArrayElementsToReturn = Integer.parseInt(SystemConfiguration.getProperty("pir.numReturnArrayElements", "1"));
    for (int i = 0; i < numArrayElementsToReturn; ++i)
    {
      if (elementList.size() > i) // we may have an element with a list rep that has fewer than numArrayElementsToReturn elements
      {
        logger.debug("Adding parts for elementArray(" + i + ") = " + elementList.get(i));
        parts.addAll(toPartitions(elementList.get(i), type));
      }
      else
      // pad with encryptions of zero
      {
        parts.addAll(getPaddedPartitions(type));
      }
    }
    return parts;
  }
}
