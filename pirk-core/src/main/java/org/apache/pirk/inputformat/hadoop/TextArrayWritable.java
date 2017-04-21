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

import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * ArrayWritable class with Text entries
 */
public class TextArrayWritable extends ArrayWritable
{
  public TextArrayWritable()
  {
    super(Text.class);
  }

  /**
   * Constructor for use when underlying array will be Text representations of String objects
   */
  public TextArrayWritable(String[] elements)
  {
    super(Text.class);

    Text[] textElements = new Text[elements.length];
    for (int i = 0; i < elements.length; i++)
    {
      textElements[i] = new Text(elements[i]);
    }
    set(textElements);
  }

  /**
   * Constructor for use when underlying array will be Text representations of BigInteger objects
   */
  public TextArrayWritable(ArrayList<BigInteger> elements)
  {
    super(Text.class);

    Text[] textElements = new Text[elements.size()];
    for (int i = 0; i < elements.size(); i++)
    {
      textElements[i] = new Text(elements.get(i).toString());
    }
    set(textElements);
  }

  /**
   * Returns the number of elements in the underlying array
   *
   */
  public int size()
  {
    return this.get().length;
  }

  /**
   * Return the ith element from the underlying array
   * <p>
   * Assumes that the underlying array consists of Text representations of BigInteger objects
   */
  public BigInteger getBigInteger(int i)
  {
    Text element = (Text) this.get()[i];
    return new BigInteger(element.toString());
  }
}
