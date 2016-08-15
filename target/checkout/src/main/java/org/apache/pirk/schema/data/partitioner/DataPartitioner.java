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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;

import org.apache.pirk.utils.PIRException;

/**
 * Interface for data partitioning
 * <p>
 * All custom object partitioners must implement this interface
 */
public interface DataPartitioner extends Serializable
{
  /**
   * Method to partition the given Object into a List of BigInteger partition elements given its type identifier.
   * <p>
   * If the Object does not have/need a specific type identifier, use null.
   */
  List<BigInteger> toPartitions(Object object, String type) throws PIRException;

  /**
   * Method to reconstruct an Object given a List of its BigInteger partition elements and its type identifier.
   * <p>
   * If the Object does not have/need a specific type identifier, use null.
   */
  Object fromPartitions(List<BigInteger> parts, int partsIndex, String type) throws PIRException;

  /**
   * Returns the number of bits of an object with the given type.
   */
  int getBits(String type) throws PIRException;

  /**
   * Creates partitions for an array of the same type of elements - used when a data value field is an array and we wish to encode these into the return value.
   */
  List<BigInteger> arrayToPartitions(List<?> elementList, String type) throws PIRException;

  /**
   * Method to get an empty set of partitions by data type - used for padding return array values.
   */
  List<BigInteger> getPaddedPartitions(String type) throws PIRException;

  /**
   * Method to get the number of partitions of the data object given the type.
   */
  int getNumPartitions(String type) throws PIRException;
}
