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
package org.apache.pirk.inputformat.hadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/**
 * ArrayWritable class with ByteWritable entries
 * 
 */
public class BytesArrayWritable extends ArrayWritable
{
  private static final Logger logger = LoggerFactory.getLogger(BytesArrayWritable.class);

  public BytesArrayWritable()
  {
    super(BytesWritable.class);
  }

  /**
   * Constructor for use when underlying array will be BytesWritable representations of byte[]
   */
  public BytesArrayWritable(byte[][] elements)
  {
    super(BytesWritable.class);

    BytesWritable[] bwElements = new BytesWritable[elements.length];
    for (int i = 0; i < elements.length; i++)
    {
      bwElements[i] = new BytesWritable(elements[i]);
    }
    set(bwElements);
  }

  /**
   * Constructor for use when underlying array will be ByteWritable representations of BigInteger objects
   */
  public BytesArrayWritable(List<BigInteger> elements)
  {
    super(BytesWritable.class);

    BytesWritable[] bwElements = new BytesWritable[elements.size()];
    for (int i = 0; i < elements.size(); i++)
    {
      byte[] bytes = elements.get(i).toByteArray();
      bwElements[i] = trim(bytes);
    }
    set(bwElements);
  }

  /**
   * Returns the number of elements in the underlying array
   */
  public int size()
  {
    return this.get().length;
  }

  /**
   * Return the ith element from the underlying array
   * <p>
   * Assumes that the underlying array consists of BytesWritable representations of BigInteger objects
   * <p>
   * Assumes that the underlying BigIntegers are unsigned, but have been stripped of zero padding (and hence the sign bit) -- must add it back in
   * 
   */
  public BigInteger getBigInteger(int i) throws IOException
  {
    BytesWritable element = (BytesWritable) this.get()[i];

    return new BigInteger(pad(element.getBytes()));
  }

  /**
   * Return the ith element as a BytesWritable
   * 
   */
  public BytesWritable get(int i)
  {
    return (BytesWritable) this.get()[i];
  }

  /**
   * Return the ith element as a byte array
   * 
   */
  public byte[] getBytes(int i)
  {
    return ((BytesWritable) this.get()[i]).getBytes();
  }

  /**
   * Return the ith element as an int
   * 
   */
  public int getByteAsInt(int i)
  {
    int retVal = 0;
    byte[] elementByteArray = this.get(i).getBytes();
    if (elementByteArray.length > 0)
    {
      retVal = elementByteArray[0] & 0xFF;
    }
    return retVal;
  }

  // Removes zero padding at the beginning of the byte array - assumes
  // big endian ordering
  private BytesWritable trim(byte[] bytes)
  {
    byte[] trimmedBytes = bytes;

    if (bytes[0] == 0)
    {
      trimmedBytes = Arrays.copyOfRange(bytes, 1, bytes.length);
    }

    return new BytesWritable(trimmedBytes);
  }

  // Adds back the zero padding at the beginning of the byte array -
  // assumes big endian ordering -- will not change value
  private byte[] pad(byte[] bytes) throws IOException
  {
    byte[] zeroByte = new byte[] {0};

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(zeroByte);
    outputStream.write(bytes);
    return outputStream.toByteArray();
  }
}
