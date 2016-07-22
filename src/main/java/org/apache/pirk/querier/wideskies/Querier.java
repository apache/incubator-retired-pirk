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
package org.apache.pirk.querier.wideskies;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.serialization.Storable;
import org.apache.pirk.utils.LogUtils;

/**
 * Class to hold the information necessary for the PIR querier to perform decryption
 * 
 */
public class Querier implements Serializable, Storable
{
  private static final long serialVersionUID = 1L;

  private static Logger logger = LogUtils.getLoggerForThisClass();

  QueryInfo queryInfo = null;

  Query query = null; // contains the query vectors and functionality

  Paillier paillier = null; // Paillier encryption functionality

  ArrayList<String> selectors = null; // selectors for the watchlist

  // map to check the embedded selectors in the results for false positives;
  // if the selector is a fixed size < 32 bits, it is included as is
  // if the selector is of variable lengths
  HashMap<Integer,String> embedSelectorMap = null;

  public Querier(QueryInfo queryInfoInput, ArrayList<String> selectorsInput, Paillier paillierInput, Query pirQueryInput,
      HashMap<Integer,String> embedSelectorMapInput)
  {
    queryInfo = queryInfoInput;

    selectors = selectorsInput;

    paillier = paillierInput;

    query = pirQueryInput;

    embedSelectorMap = embedSelectorMapInput;
  }

  public QueryInfo getPirWatchlist()
  {
    return queryInfo;
  }

  public Query getPirQuery()
  {
    return query;
  }

  public Paillier getPaillier()
  {
    return paillier;
  }

  public ArrayList<String> getSelectors()
  {
    return selectors;
  }

  public HashMap<Integer,String> getEmbedSelectorMap()
  {
    return embedSelectorMap;
  }
}
