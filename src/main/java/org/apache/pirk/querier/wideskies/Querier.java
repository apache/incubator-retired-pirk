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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold the information necessary for the PIR querier to perform decryption
 * 
 */
public class Querier implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(Querier.class);

  private QueryInfo queryInfo = null;

  private Query query = null; // contains the query vectors and functionality

  private Paillier paillier = null; // Paillier encryption functionality

  private ArrayList<String> selectors = null; // selectors for the watchlist

  // map to check the embedded selectors in the results for false positives;
  // if the selector is a fixed size < 32 bits, it is included as is
  // if the selector is of variable lengths
  private HashMap<Integer,String> embedSelectorMap = null;

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

  /**
   * Method to serialize the Querier object to a file
   */
  public void writeToFile(String filename) throws IOException
  {
    writeToFile(new File(filename));
  }

  /**
   * Method to serialize the Querier object to a file
   */
  public void writeToFile(File file) throws IOException
  {
    ObjectOutputStream oos = null;
    FileOutputStream fout = null;
    try
    {
      fout = new FileOutputStream(file, true);
      oos = new ObjectOutputStream(fout);
      oos.writeObject(this);
    } catch (Exception ex)
    {
      ex.printStackTrace();
    } finally
    {
      if (oos != null)
      {
        oos.close();
      }
      if (fout != null)
      {
        fout.close();
      }
    }
  }

  /**
   * Reconstruct the Querier object from its file serialization
   */
  public static Querier readFromFile(String filename) throws IOException
  {

    return readFromFile(new File(filename));
  }

  /**
   * Reconstruct the Querier object from its file serialization
   */
  public static Querier readFromFile(File file) throws IOException
  {
    Querier querier = null;

    FileInputStream fIn = null;
    ObjectInputStream oIn;
    try
    {
      fIn = new FileInputStream(file);
      oIn = new ObjectInputStream(fIn);
      querier = (Querier) oIn.readObject();
    } catch (IOException | ClassNotFoundException e)
    {
      e.printStackTrace();
    } finally
    {
      if (fIn != null)
      {
        try
        {
          fIn.close();
        } catch (IOException e)
        {
          e.printStackTrace();
        }
      }
    }
    return querier;
  }
}
