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
package org.apache.pirk.querier.wideskies;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.Expose;
import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.serialization.Storable;

/**
 * Class to hold the information necessary for the PIR querier to perform decryption
 */
public class Querier implements Serializable, Storable
{
  private static final long serialVersionUID = 1L;

  public static final long querierSerialVersionUID = 1L;

  @Expose
  public final long querierVersion = querierSerialVersionUID;

  @Expose
  private Query query = null; // contains the query vectors and functionality

  @Expose
  private Paillier paillier = null; // Paillier encryption functionality

  @Expose
  private List<String> selectors = null; // selectors

  // map to check the embedded selectors in the results for false positives;
  // if the selector is a fixed size < 32 bits, it is included as is
  // if the selector is of variable lengths
  @Expose
  private Map<Integer,String> embedSelectorMap = null;

  @Override public boolean equals(Object o)
  {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Querier querier = (Querier) o;

    if (querierVersion != querier.querierVersion)
      return false;
    if (!query.equals(querier.query))
      return false;
    if (!paillier.equals(querier.paillier))
      return false;
    if (!selectors.equals(querier.selectors))
      return false;
    return embedSelectorMap != null ? embedSelectorMap.equals(querier.embedSelectorMap) : querier.embedSelectorMap == null;

  }

  @Override public int hashCode()
  {
    int result = (int) (querierVersion ^ (querierVersion >>> 32));
    result = 31 * result + query.hashCode();
    result = 31 * result + paillier.hashCode();
    result = 31 * result + selectors.hashCode();
    result = 31 * result + (embedSelectorMap != null ? embedSelectorMap.hashCode() : 0);
    return result;
  }

  public Querier(List<String> selectorsInput, Paillier paillierInput, Query queryInput, Map<Integer,String> embedSelectorMapInput)
  {
    selectors = selectorsInput;

    paillier = paillierInput;

    query = queryInput;

    embedSelectorMap = embedSelectorMapInput;
  }

  public Query getQuery()
  {
    return query;
  }

  public Paillier getPaillier()
  {
    return paillier;
  }

  public List<String> getSelectors()
  {
    return selectors;
  }

  public Map<Integer,String> getEmbedSelectorMap()
  {
    return embedSelectorMap;
  }
}
