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
package org.apache.pirk.query.wideskies;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.pirk.encryption.ModPowAbstraction;
import org.apache.pirk.serialization.Storable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold the PIR query vectors
 *
 */
@JsonDeserialize(using = QueryDeserializer.class)
public class Query implements Serializable, Storable
{
  @JsonSerialize
  public static final long querySerialVersionUID = 1L;

  @JsonIgnore
  private static final Logger logger = LoggerFactory.getLogger(Query.class);

  @JsonSerialize
  private final QueryInfo queryInfo; // holds all query info

  @JsonSerialize
  private final SortedMap<Integer,BigInteger> queryElements; // query elements - ordered on insertion

  // lookup table for exponentiation of query vectors - based on dataPartitionBitSize
  // element -> <power, element^power mod N^2>
  @JsonIgnore
  private Map<BigInteger,Map<Integer,BigInteger>> expTable = new ConcurrentHashMap<>();

  // File based lookup table for modular exponentiation
  // element hash -> filename containing it's <power, element^power mod N^2> modular exponentiations
  @JsonIgnore
  private Map<Integer,String> expFileBasedLookup = new HashMap<>();

  @JsonSerialize
  private final BigInteger N; // N=pq, RSA modulus for the Paillier encryption associated with the queryElements
  @JsonSerialize
  private final BigInteger NSquared;

  public Query(QueryInfo queryInfo, BigInteger N, SortedMap<Integer,BigInteger> queryElements)
  {
    this(queryInfo, N, N.pow(2), queryElements);
  }

  public Query(QueryInfo queryInfo, BigInteger N, BigInteger NSquared, SortedMap<Integer,BigInteger> queryElements)
  {
    this.queryInfo = queryInfo;
    this.N = N;
    this.NSquared = NSquared;
    this.queryElements = queryElements;
  }

  public QueryInfo getQueryInfo()
  {
    return queryInfo;
  }

  public SortedMap<Integer,BigInteger> getQueryElements()
  {
    return queryElements;
  }

  public BigInteger getQueryElement(int index)
  {
    return queryElements.get(index);
  }

  public BigInteger getN()
  {
    return N;
  }

  public BigInteger getNSquared()
  {
    return NSquared;
  }

  public Map<Integer,String> getExpFileBasedLookup()
  {
    return expFileBasedLookup;
  }

  public String getExpFile(int i)
  {
    return expFileBasedLookup.get(i);
  }

  public void setExpFileBasedLookup(Map<Integer,String> expInput)
  {
    expFileBasedLookup = expInput;
  }

  /**
   * This should be called after all query elements have been added in order to generate the expTable. For int exponentiation with BigIntegers, assumes that
   * dataPartitionBitSize < 32.
   */
  public void generateExpTable()
  {
    int maxValue = (1 << queryInfo.getDataPartitionBitSize()) - 1; // 2^partitionBitSize - 1

    queryElements.values().parallelStream().forEach(new Consumer<BigInteger>()
    {
      @Override
      public void accept(BigInteger element)
      {
        Map<Integer,BigInteger> powMap = new HashMap<>(maxValue); // <power, element^power mod N^2>
        for (int i = 0; i <= maxValue; ++i)
        {
          BigInteger value = ModPowAbstraction.modPow(element, BigInteger.valueOf(i), NSquared);
          powMap.put(i, value);
        }
        expTable.put(element, powMap);
      }
    });
    logger.debug("expTable.size() = " + expTable.keySet().size() + " NSquared = " + NSquared.intValue() + " = " + NSquared.toString());
  }

  public BigInteger getExp(BigInteger value, int power)
  {
    Map<Integer,BigInteger> powerMap = expTable.get(value);
    return (powerMap == null) ? null : powerMap.get(power);
  }
}
