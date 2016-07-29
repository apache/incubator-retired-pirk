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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.util.ByteArrayBuffer;
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

    ArrayList<BigInteger> partitions = new ArrayList<>();
    if (value.bitLength() < partitionSize)
    {
      partitions.add(value);
    }
    else
    {
      int bitLength = value.bitLength();
      mask = mask.shiftLeft(bitLength - partitionSize); // shift left for big endian partitioning

      BigInteger result;
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
    return (BigInteger.valueOf(2).pow(partitionSize)).subtract(BigInteger.ONE);
  }

  /**
   * Method to get the number of 8-bit partitions given the element type
   * 
   */
  @Override
  public int getNumPartitions(String type) throws PIRException
  {
    int partitionSize = 8;

    int numParts;
    switch (type)
    {
      case BYTE:
        numParts = Byte.SIZE / partitionSize;
        break;
      case SHORT:
        numParts = Short.SIZE / partitionSize;
        break;
      case INT:
        numParts = Integer.SIZE / partitionSize;
        break;
      case LONG:
        numParts = Long.SIZE / partitionSize;
        break;
      case FLOAT:
        numParts = Float.SIZE / partitionSize;
        break;
      case DOUBLE:
        numParts = Double.SIZE / partitionSize;
        break;
      case CHAR:
        numParts = Character.SIZE / partitionSize;
        break;
      case STRING:
        numParts = Integer.parseInt(SystemConfiguration.getProperty("pir.stringBits")) / partitionSize;
        break;
      default:
        throw new PIRException("type = " + type + " not recognized!");
    }
    return numParts;
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
  public Object fromPartitions(ArrayList<BigInteger> parts, int partsIndex, String type) throws PIRException
  {
    Object element;

    switch (type)
    {
      case BYTE:
        BigInteger bInt = parts.get(partsIndex);
        element = Byte.valueOf(bInt.toString());
        break;
      case SHORT:
      {
        byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
        element = ByteBuffer.wrap(bytes).getShort();
        break;
      }
      case INT:
      {
        byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
        element = ByteBuffer.wrap(bytes).getInt();
        break;
      }
      case LONG:
      {
        byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
        element = ByteBuffer.wrap(bytes).getLong();
        break;
      }
      case FLOAT:
      {
        byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
        element = ByteBuffer.wrap(bytes).getFloat();
        break;
      }
      case DOUBLE:
      {
        byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
        element = ByteBuffer.wrap(bytes).getDouble();
        break;
      }
      case CHAR:
      {
        byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
        element = ByteBuffer.wrap(bytes).getChar();
        break;
      }
      case STRING:
      {
        byte[] bytes = appendBytes(parts, partsIndex, getNumPartitions(type));
        element = new String(bytes).trim(); // this should remove 0 padding added for partitioning underflowing strings

        break;
      }
      default:
        throw new PIRException("type = " + type + " not recognized!");
    }
    return element;
  }

  private byte[] appendBytes(ArrayList<BigInteger> parts, int partsIndex, int numParts)
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
  public ArrayList<BigInteger> toPartitions(Object obj, String type) throws PIRException
  {
    ArrayList<BigInteger> parts = new ArrayList<>();

    int numParts = getNumPartitions(type);
    switch (type)
    {
      case BYTE:
        if (obj instanceof String)
        {
          parts.add(new BigInteger(ByteBuffer.allocate(1).put(Byte.parseByte((String) obj)).array()));
        }
        else
        {
          parts.add(new BigInteger(ByteBuffer.allocate(1).put((byte) obj).array()));
        }
        break;
      case STRING:
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
        break;
      default:
        // Extract the byte array
        byte[] bytes = new byte[0];
        switch (type)
        {
          case SHORT:
            if (obj instanceof String)
            {
              bytes = ByteBuffer.allocate(numParts).putShort(Short.parseShort((String) obj)).array();
            }
            else
            {
              bytes = ByteBuffer.allocate(numParts).putShort((short) obj).array();
            }
            break;
          case INT:
            if (obj instanceof String)
            {
              bytes = ByteBuffer.allocate(numParts).putInt(Integer.parseInt((String) obj)).array();
            }
            else
            {
              bytes = ByteBuffer.allocate(numParts).putInt((int) obj).array();
            }
            break;
          case LONG:
            if (obj instanceof String)
            {
              bytes = ByteBuffer.allocate(numParts).putLong(Long.parseLong((String) obj)).array();
            }
            else
            {
              bytes = ByteBuffer.allocate(numParts).putLong((long) obj).array();
            }
            break;
          case FLOAT:
            if (obj instanceof String)
            {
              bytes = ByteBuffer.allocate(numParts).putFloat(Float.parseFloat((String) obj)).array();
            }
            else
            {
              bytes = ByteBuffer.allocate(numParts).putFloat((float) obj).array();
            }
            break;
          case DOUBLE:
            if (obj instanceof String)
            {
              bytes = ByteBuffer.allocate(numParts).putDouble(Double.parseDouble((String) obj)).array();
            }
            else
            {
              bytes = ByteBuffer.allocate(numParts).putDouble((double) obj).array();
            }
            break;
          case CHAR:
            if (obj instanceof String)
            {
              bytes = ByteBuffer.allocate(numParts).putChar(((String) (obj)).charAt(0)).array();
            }
            else
            {
              bytes = ByteBuffer.allocate(numParts).putChar((char) obj).array();
            }
            break;
        }

        // Add bytes to parts ArrayList
        for (byte b : bytes)
        {
          // Make sure that BigInteger treats the byte as 'unsigned' literal
          parts.add(BigInteger.valueOf(((long) b) & 0xFF));
        }
        break;
    }

    return parts;
  }

  /**
   * Method to get an empty set of partitions by data type - used for padding return array values
   */
  @Override
  public ArrayList<BigInteger> getPaddedPartitions(String type) throws PIRException
  {
    ArrayList<BigInteger> parts = new ArrayList<>();

    int numParts = getNumPartitions(type);
    switch (type)
    {
      case BYTE:
        parts.add(new BigInteger(ByteBuffer.allocate(1).put(Byte.parseByte("0")).array()));
        break;
      case STRING:
        for (int i = 0; i < numParts; ++i)
        {
          parts.add(new BigInteger(ByteBuffer.allocate(1).put(Byte.parseByte("0")).array()));
        }
        break;
      default:
        // Extract the byte array
        byte[] bytes = new byte[0];
        switch (type)
        {
          case SHORT:
            bytes = ByteBuffer.allocate(numParts).putShort(Short.parseShort("0")).array();
            break;
          case INT:
            bytes = ByteBuffer.allocate(numParts).putInt(Integer.parseInt("0")).array();
            break;
          case LONG:
            bytes = ByteBuffer.allocate(numParts).putLong(Long.parseLong("0")).array();
            break;
          case FLOAT:
            bytes = ByteBuffer.allocate(numParts).putFloat(Float.parseFloat("0")).array();
            break;
          case DOUBLE:
            bytes = ByteBuffer.allocate(numParts).putDouble(Double.parseDouble("0")).array();
            break;
          case CHAR:
            bytes = ByteBuffer.allocate(numParts).putChar('0').array();
            break;
        }

        // Add bytes to parts ArrayList
        for (byte b : bytes)
        {
          parts.add(new BigInteger(ByteBuffer.allocate(1).put(b).array()));
        }
        break;
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
