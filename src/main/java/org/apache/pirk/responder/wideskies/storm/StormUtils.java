/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.apache.pirk.responder.wideskies.storm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.query.QuerySchemaLoader;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.apache.pirk.serialization.LocalFileSystemStore;
import org.apache.pirk.utils.SystemConfiguration;
import org.apache.storm.Constants;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Map;

/**
 * Utils class for the Storm implementation of Wideskies
 */
public class StormUtils
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(StormUtils.class);

  /**
   * Method to read in serialized Query object from the given queryFile
   * 
   * @param useHdfs
   * @param hdfsUri
   * @param queryFile
   * @return
   */
  public static Query getQuery(boolean useHdfs, String hdfsUri, String queryFile)
  {
    Query query = null;

    try
    {
      if (useHdfs)
      {
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
        logger.info("reading query file from hdfs: " + queryFile);
        query = (new HadoopFileSystemStore(fs)).recall(queryFile, Query.class);
      }
      else
      {
        logger.info("reading local query file from " + queryFile);
        query = (new LocalFileSystemStore()).recall(queryFile, Query.class);
      }
    } catch (Exception e)
    {
      logger.error("Unable to initalize query info.", e);
      throw new RuntimeException(e);
    }
    return query;
  }

  /**
   * Method to read in and return a serialized Query object from the given file and initialize/load the query.schemas and data.schemas
   * 
   * @param map
   * @return
   */
  public static Query prepareQuery(Map map)
  {
    Query query = null;

    boolean useHdfs = (boolean) map.get(StormConstants.USE_HDFS);
    String hdfsUri = (String) map.get(StormConstants.HDFS_URI_KEY);
    String queryFile = (String) map.get(StormConstants.QUERY_FILE_KEY);
    try
    {
      query = StormUtils.getQuery(useHdfs, hdfsUri, queryFile);

    } catch (Exception e)
    {
      logger.warn("Unable to initialize query info.", e);
    }

    return query;
  }

  /***
   * Initialize data and query schema. Conf requires values for USE_HDFS, HDFS_URI_KEY, DSCHEMA_KEY, and QSCHEMA_KEY
   * @param conf
   */
  public static void initializeSchemas(Map conf, String id)
  {
    SystemConfiguration.setProperty("data.schemas", (String) conf.get(StormConstants.DSCHEMA_KEY));
    SystemConfiguration.setProperty("query.schemas", (String) conf.get(StormConstants.QSCHEMA_KEY));

    try
    {
      boolean hdfs = (boolean) conf.get(StormConstants.USE_HDFS);
      if (hdfs)
      {
        String hdfsUri = (String) conf.get(StormConstants.HDFS_URI_KEY);
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
        DataSchemaLoader.initialize(true, fs);
        QuerySchemaLoader.initialize(true, fs);
      }
      else
      {
        DataSchemaLoader.initialize();
        QuerySchemaLoader.initialize();
      }
    } catch (Exception e)
    {
      logger.error("Failed to initialize schema files.", e);
      throw new RuntimeException(e);
    }
  }


  protected static boolean isTickTuple(Tuple tuple)
  {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
  }

}
