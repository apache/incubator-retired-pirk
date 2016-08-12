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
package org.apache.pirk.schema.data.partitioner;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for partitioning objects with primitive Java types
 * 
 */
public class PrimitiveTypePartitioner implements DataPartitioner
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(PrimitiveTypePartitioner.class);

  public static final String BYTE = "byte";
  public static final String SHORT = "short";
  public static final String INT = "int";
  public static final String LONG = "long";
  public static final String FLOAT = "float";
  public static final String DOUBLE = "double";
  public static final String CHAR = "char";
  public static final String STRING = "string";

  /**
   * Splits the given BigInteger into partitions given by the partitionSize
   *
   */
  public static List<BigInteger> partitionBits(BigInteger value, int partitionSize, BigInteger mask) throws PIRException
  {
    if (mask.bitLength() != partitionSize)
    {
      throw new PIRException("mask.bitLength() " + mask.bitLength() + " != partitionSize = " + partitionSize);
    }

    List<BigInteger> partitions = new ArrayList<>();
    if (value.bitLength() < partitionSize)
    {
      partitions.add(value);
    }
    else
    {
      int bitLength = value.bitLength();
      mask = mask.shiftLeft(bitLength - partitionSize); // shift left for big endian partitioning

      int partNum = 0;
      for (int i = 0; i < bitLength; i += partitionSize)
      {
        BigInteger result = value.and(mask);

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
    return BigInteger.valueOf(2).pow(partitionSize).subtract(BigInteger.ONE);
  }

  /**
   * Method to get the number of 8-bit partitions given the element type
   * 
   */
  @Override
  public int getNumPartitions(String type) throws PIRException
  {
    return getBits(type) / 8;
  }

  /**
   * Get the bit size of the allowed primitive java types
   */
  @Override
  public int getBits(String type) throws PIRException
  {
    int bits;
    switch (type)
    {
      case BYTE:
        bits = Byte.SIZE;
        break;
      case SHORT:
        bits = Short.SIZE;
        break;
      case INT:
        bits = Integer.SIZE;
        break;
      case LONG:
        bits = Long.SIZE;
        break;
      case FLOAT:
        bits = Float.SIZE;
        break;
      case DOUBLE:
        bits = Double.SIZE;
        break;
      case CHAR:
        bits = Character.SIZE;
        break;
      case STRING:
        bits = Integer.parseInt(SystemConfiguration.getProperty("pir.stringBits"));
        break;
      default:
        throw new PIRException("type = " + type + " not recognized!");
    }
    return bits;
  }

  /**
   * Reconstructs the object from the partitions
   */
  @Override
  public Object fromPartitions(List<BigInteger> parts, int partsIndex, String type) throws PIRException
  {
    Object element;

    switch (type)
    {
      case BYTE:
        element = parts.get(partsIndex).byteValueExact();
        break;
      case SHORT:
      {
        byte[] bytes = partsToBytes(parts, partsIndex, type);
        element = bytesToShort(bytes);
        break;
      }
      case INT:
      {
        byte[] bytes = partsToBytes(parts, partsIndex, type);
        element = bytesToInt(bytes);
        break;
      }
      case LONG:
      {
        byte[] bytes = partsToBytes(parts, partsIndex, type);
        element = bytesToLong(bytes);
        break;
      }
      case FLOAT:
      {
        byte[] bytes = partsToBytes(parts, partsIndex, type);
        element = Float.intBitsToFloat(bytesToInt(bytes));
        break;
      }
      case DOUBLE:
      {
        byte[] bytes = partsToBytes(parts, partsIndex, type);
        element = Double.longBitsToDouble(bytesToLong(bytes));
        break;
      }
      case CHAR:
      {
        byte[] bytes = partsToBytes(parts, partsIndex, type);
        element = (char) bytesToShort(bytes);
        break;
      }
      case STRING:
      {
        byte[] bytes = partsToBytes(parts, partsIndex, type);
        try
        {
          // This should remove 0 padding added for partitioning underflowing strings.
          element = new String(bytes, "UTF-8").trim();
        } catch (UnsupportedEncodingException e)
        {
          // UTF-8 is a required encoding.
          throw new RuntimeException(e);
        }
        break;
      }
      default:
        throw new PIRException("type = " + type + " not recognized!");
    }

    return element;
  }

  private byte[] partsToBytes(List<BigInteger> parts, int partsIndex, String type) throws PIRException
  {
    int numParts = getNumPartitions(type);
    byte[] result = new byte[numParts];
    for (int i = 0; i < numParts; ++i)
    {
      result[i] = parts.get(partsIndex + i).byteValue();
    }
    return result;
  }

  /**
   * 
   * Partitions an object to an ArrayList of BigInteger values, currently represents an 8-bit partitioning
   */
  @Override
  public ArrayList<BigInteger> toPartitions(Object obj, String type) throws PIRException
  {
    ArrayList<BigInteger> parts = new ArrayList<>();

    byte[] bytes = new byte[0];

    switch (type)
    {
      case BYTE:
        byte value = obj instanceof String ? Byte.parseByte((String) obj) : (byte) obj;
        bytes = new byte[] {value};
        break;
      case CHAR:
        char cvalue = obj instanceof String ? ((String) obj).charAt(0) : (char) obj;
        bytes = shortToBytes((short) cvalue);
        break;
      case SHORT:
        short svalue = obj instanceof String ? Short.parseShort((String) obj) : (short) obj;
        bytes = shortToBytes(svalue);
        break;
      case INT:
        int ivalue = obj instanceof String ? Integer.parseInt((String) obj) : (int) obj;
        bytes = intToBytes(ivalue);
        break;
      case LONG:
        long lvalue = obj instanceof String ? Long.parseLong((String) obj) : (long) obj;
        bytes = longToBytes(lvalue);
        break;
      case FLOAT:
        float fvalue = obj instanceof String ? Float.parseFloat((String) obj) : (float) obj;
        bytes = intToBytes(Float.floatToRawIntBits(fvalue));
        break;
      case DOUBLE:
        double dvalue = obj instanceof String ? Double.parseDouble((String) obj) : (double) obj;
        bytes = longToBytes(Double.doubleToRawLongBits(dvalue));
        break;
      case STRING:
        byte[] stringBytes;
        try
        {
          stringBytes = ((String) obj).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e)
        {
          // UTF-8 is a required encoding.
          throw new RuntimeException(e);
        }
        for (int i = 0; i < getNumPartitions(STRING); ++i)
        {
          if (i < stringBytes.length)
          {
            parts.add(BigInteger.valueOf((long) stringBytes[i] & 0xFF));
          }
          else
          {
            parts.add(BigInteger.ZERO);
          }
        }
        break;
      default:
        throw new PIRException("type = " + type + " not recognized!");
    }

    // Add any bytes to parts list.
    for (byte b : bytes)
    {
      // Make sure that BigInteger treats the byte as 'unsigned' literal
      parts.add(BigInteger.valueOf((long) b & 0xFF));
    }

    return parts;
  }

  /**
   * Method to get an empty set of partitions by data type - used for padding return array values
   */
  @Override
  public List<BigInteger> getPaddedPartitions(String type) throws PIRException
  {
    int numParts = getNumPartitions(type);

    List<BigInteger> parts = new ArrayList<>(numParts);
    for (int i = 0; i < numParts; i++)
    {
      parts.add(BigInteger.ZERO);
    }
    return parts;
  }

  /**
   * Create partitions for an array of the same type of elements - used when a data value field is an array and we wish to encode these into the return value
   */
  @Override
  public ArrayList<BigInteger> arrayToPartitions(List<?> elementList, String type) throws PIRException
  {
    ArrayList<BigInteger> parts = new ArrayList<>();

    int numArrayElementsToReturn = SystemConfiguration.getIntProperty("pir.numReturnArrayElements", 1);
    for (int i = 0; i < numArrayElementsToReturn; ++i)
    {
      if (elementList.size() > i) // we may have an element with a list rep that has fewer than numArrayElementsToReturn elements
      {
        logger.debug("Adding parts for elementArray(" + i + ") = " + elementList.get(i));
        parts.addAll(toPartitions(elementList.get(i), type));
      }
      else
      {
        // Pad with encryptions of zero.
        parts.addAll(getPaddedPartitions(type));
      }
    }
    return parts;
  }

  // Helpers to return the given numbers in network byte order representation.

  private byte[] shortToBytes(short value)
  {
    return new byte[] {
        (byte) (value >> 8),
        (byte) value};
  }

  private short bytesToShort(byte[] bytes)
  {
    return (short)(
        bytes[0] << 8 |
        bytes[1] & 0xff);
  }

  private byte[] intToBytes(int value)
  {
    return new byte[] {
        (byte) (value >> 24), 
        (byte) (value >> 16), 
        (byte) (value >> 8), 
        (byte) value};
  }
  
  private int bytesToInt(byte[] bytes)
  {
    return
        (bytes[0] << 24) |
        (bytes[1] & 0xff) << 16 |
        (bytes[2] & 0xff) << 8 |
        (bytes[3] & 0xff);
  }
  
  private byte[] longToBytes(long value)
  {
    return new byte[] {
        (byte) (value >> 56),
        (byte) (value >> 48),
        (byte) (value >> 40),
        (byte) (value >> 32),
        (byte) (value >> 24),
        (byte) (value >> 16),
        (byte) (value >> 8),
        (byte) value};
  }
  
  private long bytesToLong(byte[] bytes)
  {
    return
         (long)bytes[0] << 56 |
        ((long)bytes[1] & 0xff) << 48 |
        ((long)bytes[2] & 0xff) << 40 |
        ((long)bytes[3] & 0xff) << 32 |
        ((long)bytes[4] & 0xff) << 24 |
        ((long)bytes[5] & 0xff) << 16 |
        ((long)bytes[6] & 0xff) << 8 |
         (long)bytes[7] & 0xff;
  }
}
