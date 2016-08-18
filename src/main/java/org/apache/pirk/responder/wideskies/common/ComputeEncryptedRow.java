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
package org.apache.pirk.responder.wideskies.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pirk.encryption.IntegerMathAbstraction;
import org.apache.pirk.inputformat.hadoop.BytesArrayWritable;
import org.apache.pirk.query.wideskies.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Class to compute the encrypted row elements for a query from extracted data partitions
 * 
 */
public class ComputeEncryptedRow
{
  private static final Logger logger = LoggerFactory.getLogger(ComputeEncryptedRow.class);

  // Input: base, exponent, NSquared
  // <<base,exponent,NSquared>, base^exponent mod N^2>
  private static LoadingCache<Tuple3<BigInteger,BigInteger,BigInteger>,BigInteger> expCache = CacheBuilder.newBuilder().maximumSize(10000)
      .build(new CacheLoader<Tuple3<BigInteger,BigInteger,BigInteger>,BigInteger>()
      {
        @Override
        public BigInteger load(Tuple3<BigInteger,BigInteger,BigInteger> info) throws Exception
        {
          logger.debug("cache miss");
          return IntegerMathAbstraction.modPow(info._1(), info._2(), info._3());
        }
      });

  /**
   * Populate the cache based on the pre-generated exp table in hdfs
   */
  public static void loadCacheFromHDFS(FileSystem fs, String hdfsFileName, Query query) throws IOException
  {
    logger.info("Loading cache from hdfsFileName = " + hdfsFileName);

    Path expPath = new Path(hdfsFileName);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(expPath))))
    {
      String line;
      while ((line = br.readLine()) != null) // form: element_hash,<exponent>-<element^exponent mod N^2>
      {
        String[] rowValTokens = line.split(",");
        BigInteger base = query.getQueryElement(Integer.parseInt(rowValTokens[0]));

        String[] expMod = rowValTokens[1].split("-");
        BigInteger exponent = new BigInteger(expMod[0]);
        BigInteger value = new BigInteger(expMod[1]);

        // Cache: <<base,exponent,NSquared>, base^exponent mod N^2>
        Tuple3<BigInteger,BigInteger,BigInteger> key = new Tuple3<>(base, exponent, query.getNSquared());
        expCache.put(key, value);
      }
    }
  }

  /**
   * Method to compute the encrypted row elements for a query from extracted data partitions in the form of Iterable{@link <BytesArrayWritable>}
   * <p>
   * For each row (as indicated by key = hash(selector)), iterates over the dataPartitions and calculates the column values.
   * <p>
   * Optionally uses a static LRU cache for the modular exponentiation
   * <p>
   * Emits {@code Tuple2<<colNum, colVal>>}
   * 
   */
  public static List<Tuple2<Long,BigInteger>> computeEncRow(Iterable<BytesArrayWritable> dataPartitionsIter, Query query, int rowIndex,
      boolean limitHitsPerSelector, int maxHitsPerSelector, boolean useCache) throws IOException
  {
    List<Tuple2<Long,BigInteger>> returnPairs = new ArrayList<>();

    // Pull the corresponding encrypted row query
    BigInteger rowQuery = query.getQueryElement(rowIndex);

    long colCounter = 0;
    int elementCounter = 0;
    for (BytesArrayWritable dataPartitions : dataPartitionsIter)
    {
      logger.debug("rowIndex = " + rowIndex + " elementCounter = " + elementCounter);

      if (limitHitsPerSelector)
      {
        if (elementCounter >= maxHitsPerSelector)
        {
          break;
        }
      }
      logger.debug("dataPartitions.size() = " + dataPartitions.size() + " rowIndex = " + rowIndex + " colCounter = " + colCounter);

      // Update the associated column values
      for (int i = 0; i < dataPartitions.size(); ++i)
      {
        BigInteger part = dataPartitions.getBigInteger(i);
        BigInteger exp = null;
        try
        {
          if (useCache)
          {
            exp = expCache.get(new Tuple3<>(rowQuery, part, query.getNSquared()));
          }
          else
          {
            exp = IntegerMathAbstraction.modPow(rowQuery, part, query.getNSquared());
          }
        } catch (ExecutionException e)
        {
          e.printStackTrace();
        }
        logger.debug("rowIndex = " + rowIndex + " colCounter = " + colCounter + " part = " + part.toString() + " part binary = " + part.toString(2) + " exp = "
            + exp + " i = " + i + " partition = " + dataPartitions.getBigInteger(i) + " = " + dataPartitions.getBigInteger(i).toString(2));

        returnPairs.add(new Tuple2<>(colCounter, exp));

        ++colCounter;
      }
      ++elementCounter;
    }
    return returnPairs;
  }

  /**
   * Method to compute the encrypted row elements for a query from extracted data partitions in the form of Iterable{@link List<BigInteger> * * * * }
   * <p>
   * For each row (as indicated by key = hash(selector)), iterates over the dataPartitions and calculates the column values.
   * <p>
   * Optionally uses a static LRU cache for the modular exponentiation
   * <p>
   * Emits {@code Tuple2<<colNum, colVal>>}
   * 
   */
  public static List<Tuple2<Long,BigInteger>> computeEncRowBI(Iterable<List<BigInteger>> dataPartitionsIter, Query query, int rowIndex,
      boolean limitHitsPerSelector, int maxHitsPerSelector, boolean useCache) throws IOException
  {
    List<Tuple2<Long,BigInteger>> returnPairs = new ArrayList<>();

    // Pull the corresponding encrypted row query
    BigInteger rowQuery = query.getQueryElement(rowIndex);

    long colCounter = 0;
    int elementCounter = 0;
    for (List<BigInteger> dataPartitions : dataPartitionsIter)
    {
      // long startTime = System.currentTimeMillis();

      logger.debug("rowIndex = " + rowIndex + " elementCounter = " + elementCounter);

      if (limitHitsPerSelector)
      {
        if (elementCounter >= maxHitsPerSelector)
        {
          logger.info("maxHits: rowIndex = " + rowIndex + " elementCounter = " + elementCounter);
          break;
        }
      }
      logger.debug("dataPartitions.size() = " + dataPartitions.size() + " rowIndex = " + rowIndex + " colCounter = " + colCounter);

      // Update the associated column values
      for (int i = 0; i < dataPartitions.size(); ++i)
      {
        BigInteger part = dataPartitions.get(i);
        BigInteger exp = null;
        try
        {
          if (useCache)
          {
            exp = expCache.get(new Tuple3<>(rowQuery, part, query.getNSquared()));
          }
          else
          {
            exp = IntegerMathAbstraction.modPow(rowQuery, part, query.getNSquared());
          }
        } catch (ExecutionException e)
        {
          e.printStackTrace();
        }
        logger.debug("rowIndex = " + rowIndex + " colCounter = " + colCounter + " part = " + part.toString() + " part binary = " + part.toString(2) + " exp = "
            + exp + " i = " + i);

        returnPairs.add(new Tuple2<>(colCounter, exp));

        ++colCounter;
      }

      // long endTime = System.currentTimeMillis();
      // logger.debug("Completed encrypting row data element = " + rowIndex + " duration = " + (endTime - startTime));

      ++elementCounter;
    }
    logger.info("totalHits: rowIndex = " + rowIndex + " elementCounter = " + elementCounter);

    return returnPairs;
  }

  /**
   * Method to compute the encrypted row elements for a query from extracted data partitions in the form of Iterable{@link <BytesArrayWritable> * * * * } given
   * an input modular exponentiation table for the row
   * <p>
   * For each row (as indicated by key = hash(selector)), iterates over the dataPartitions and calculates the column values.
   * <p>
   * Emits {@code Tuple2<<colNum, colVal>>}
   * 
   */
  public static List<Tuple2<Long,BigInteger>> computeEncRowCacheInput(Iterable<List<BigInteger>> dataPartitionsIter,
      HashMap<Integer,BigInteger> cache, int rowIndex, boolean limitHitsPerSelector, int maxHitsPerSelector) throws IOException
  {
    List<Tuple2<Long,BigInteger>> returnPairs = new ArrayList<>();

    long colCounter = 0;
    int elementCounter = 0;
    for (List<BigInteger> dataPartitions : dataPartitionsIter)
    {
      logger.debug("elementCounter = " + elementCounter);

      if (limitHitsPerSelector)
      {
        if (elementCounter >= maxHitsPerSelector)
        {
          break;
        }
      }
      logger.debug("dataPartitions.size() = " + dataPartitions.size() + " rowIndex = " + rowIndex + " colCounter = " + colCounter);

      // Update the associated column values
      for (int i = 0; i < dataPartitions.size(); ++i)
      {
        BigInteger part = dataPartitions.get(i);
        BigInteger exp = cache.get(part.intValue());

        logger.debug("rowIndex = " + rowIndex + " colCounter = " + colCounter + " part = " + part.toString() + " exp = " + exp + " i = " + i);

        returnPairs.add(new Tuple2<>(colCounter, exp));

        ++colCounter;
      }
      ++elementCounter;
    }
    return returnPairs;
  }

  /**
   * Method to compute the encrypted row elements for a query from extracted data partitions in the form of BytesArrayWritable
   * <p>
   * For each row (as indicated by key = hash(selector)), iterates over the dataPartitions and calculates the column values.
   * <p>
   * Uses a static LRU cache for the modular exponentiation
   * <p>
   * Caller is responsible for keeping track of the colIndex and the the maxHitsPerSelector values
   * <p>
   * Emits {@code Tuple2<<colNum, colVal>>}
   * 
   */
  public static List<Tuple2<Long,BigInteger>> computeEncRow(BytesArrayWritable dataPartitions, Query query, int rowIndex, int colIndex) throws IOException
  {
    List<Tuple2<Long,BigInteger>> returnPairs = new ArrayList<>();

    // Pull the corresponding encrypted row query
    BigInteger rowQuery = query.getQueryElement(rowIndex);

    // Initialize the column counter
    long colCounter = colIndex;

    logger.debug("dataPartitions.size() = " + dataPartitions.size() + " rowIndex = " + rowIndex + " colCounter = " + colCounter);

    // Update the associated column values
    for (int i = 0; i < dataPartitions.size(); ++i)
    {
      BigInteger part = dataPartitions.getBigInteger(i);

      BigInteger exp = null;
      try
      {
        exp = expCache.get(new Tuple3<>(rowQuery, part, query.getNSquared()));
      } catch (ExecutionException e)
      {
        e.printStackTrace();
      }

      logger.debug("rowIndex = " + rowIndex + " colCounter = " + colCounter + " part = " + part.toString() + " part binary = " + part.toString(2) + " exp = "
          + exp + " i = " + i + " partition = " + dataPartitions.getBigInteger(i) + " = " + dataPartitions.getBigInteger(i).toString(2));

      returnPairs.add(new Tuple2<>(colCounter, exp));

      ++colCounter;
    }

    return returnPairs;
  }
}
