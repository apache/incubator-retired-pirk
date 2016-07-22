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
package org.apache.pirk.responder.wideskies.spark;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.apache.pirk.utils.LogUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Class to compute the distributed lookup table for the modular exponentiations used in performing a query
 * 
 */
public class ComputeExpLookupTable
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  /**
   * Method to create the distributed modular exponentiation lookup table in hdfs for a given Query
   * <p>
   * Returns a Pair RDD of the form <queryHash, <<power>,<element^power mod N^2>>
   */
  public static JavaPairRDD<Integer,Iterable<Tuple2<Integer,BigInteger>>> computeExpTable(JavaSparkContext sc, FileSystem fs, BroadcastVars bVars, Query query,
      String queryInputFile, String outputDirExp)
  {
    return computeExpTable(sc, fs, bVars, query, queryInputFile, outputDirExp, false);
  }

  /**
   * Method to create the distributed modular exponentiation lookup table in hdfs for a given Query
   * <p>
   * Returns a Pair RDD of the form <queryHash, <<power>,<element^power mod N^2>>
   */
  public static JavaPairRDD<Integer,Iterable<Tuple2<Integer,BigInteger>>> computeExpTable(JavaSparkContext sc, FileSystem fs, BroadcastVars bVars, Query query,
      String queryInputFile, String outputDirExp, boolean useModExpJoin)
  {
    JavaPairRDD<Integer,Iterable<Tuple2<Integer,BigInteger>>> expCalculations = null;

    logger.info("Creating expTable in hdfs for queryName = " + query.getQueryInfo().getQueryName());

    // Prep the output directory
    Path outPathExp = new Path(outputDirExp);
    try
    {
      if (fs.exists(outPathExp))
      {
        fs.delete(outPathExp, true);
      }
    } catch (IOException e)
    {
      e.printStackTrace();
    }

    // Write the query hashes to a RDD
    TreeMap<Integer,BigInteger> queryElements = query.getQueryElements();
    ArrayList<Integer> keys = new ArrayList<Integer>(queryElements.keySet());

    int numSplits = Integer.parseInt(SystemConfiguration.getProperty("pir.expCreationSplits", "100"));
    JavaRDD<Integer> queryHashes = sc.parallelize(keys, numSplits);

    // Generate the exp table
    // <queryHash, <<power>,<element^power mod N^2>>
    int numExpLookupPartitions = Integer.parseInt(SystemConfiguration.getProperty("pir.numExpLookupPartitions", "100"));
    expCalculations = queryHashes.flatMapToPair(new ExpTableGenerator(bVars)).groupByKey(numExpLookupPartitions);

    if (!useModExpJoin)
    {
      // Generate the queryHash -> filename mapping and write the exp table to hdfs
      JavaPairRDD<Integer,String> hashToPartition = expCalculations.mapPartitionsToPair(new ExpKeyFilenameMap(bVars));

      // Place exp table in query object and in the BroadcastVars
      Map<Integer,String> queryHashFileNameMap = hashToPartition.collectAsMap();
      query.setExpFileBasedLookup(new HashMap<Integer,String>(queryHashFileNameMap));
      try
      {
        new HadoopFileSystemStore(fs).store(queryInputFile, query);
      } catch (IOException e)
      {
        e.printStackTrace();
      }
      bVars.setQuery(query);
    }

    logger.info("Completed creation of expTable");

    return expCalculations;
  }
}
