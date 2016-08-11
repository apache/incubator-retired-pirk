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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pirk.utils.ISO8601DateParser;
import org.apache.pirk.utils.PIRException;

/**
 * Partitioner class for ISO8061 dates
 * <p>
 * Assumes that the dates are passed to the partitioner in String format
 */
public class ISO8601DatePartitioner implements DataPartitioner
{
  private static final long serialVersionUID = 1L;

  private PrimitiveTypePartitioner ptp = null;

  public ISO8601DatePartitioner()
  {
    ptp = new PrimitiveTypePartitioner();
  }

  @Override
  public ArrayList<BigInteger> toPartitions(Object object, String type) throws PIRException
  {
    long dateLongFormat;
    try
    {
      dateLongFormat = ISO8601DateParser.getLongDate((String) object);
    } catch (ParseException e)
    {
      throw new PIRException("Unable to parse ISO8601 date " + object, e);
    }

    return ptp.toPartitions(dateLongFormat, PrimitiveTypePartitioner.LONG);
  }

  @Override
  public Object fromPartitions(List<BigInteger> parts, int partsIndex, String type) throws PIRException
  {
    long dateLongFormat = (long) ptp.fromPartitions(parts, partsIndex, PrimitiveTypePartitioner.LONG);

    return ISO8601DateParser.fromLongDate(dateLongFormat);
  }

  @Override
  public int getBits(String type)
  {
    return Long.SIZE;
  }

  @Override
  public ArrayList<BigInteger> arrayToPartitions(List<?> elementList, String type) throws PIRException
  {
    return ptp.arrayToPartitions(elementList, PrimitiveTypePartitioner.LONG);
  }

  @Override
  public ArrayList<BigInteger> getPaddedPartitions(String type) throws PIRException
  {
    return ptp.getPaddedPartitions(PrimitiveTypePartitioner.LONG);
  }

  @Override
  public int getNumPartitions(String type) throws PIRException
  {
    return ptp.getNumPartitions(PrimitiveTypePartitioner.LONG);
  }
}
