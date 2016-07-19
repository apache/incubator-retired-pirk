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
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.schema.data.LoadDataSchemas;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.apache.pirk.utils.LogUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.apache.storm.Constants;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.net.URI;
import java.util.Map;

public class StormUtils
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  public static Query getQuery(boolean useHdfs, String hdfsUri, String queryFile)
  {

    Query query = null;

    try
    {
      if (useHdfs)
      {
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
        logger.info("reading query file from hdfs: " + queryFile);

        query = Query.readFromHDFSFile(new Path(queryFile), fs);
      }
      else
      {
        logger.info("reading local query file from " + queryFile);
        query = Query.readFromFile(new File(queryFile));
      }
    } catch (Exception e)
    {
      logger.warn("Unable to initalize query info.", e);
    }
    return query;
  }

  public static Query prepareQuery(Map map)
  {
    Query query = null;

    boolean useHdfs = (boolean) map.get(StormConstants.USE_HDFS);
    String hdfsUri = (String) map.get(StormConstants.HDFS_URI_KEY);
    String queryFile = (String) map.get(StormConstants.QUERY_FILE_KEY);
    try
    {
      StormUtils.initializeSchemas(map);
      query = StormUtils.getQuery(useHdfs, hdfsUri, queryFile);

    } catch (Exception e)
    {
      logger.warn("Unable to initialize query info.", e);
    }

    return query;
  }

  public static void initializeSchemas(Map map) throws Exception
  {
    SystemConfiguration.setProperty("data.schemas", (String) map.get(StormConstants.DSCHEMA_KEY));
    SystemConfiguration.setProperty("query.schemas", (String) map.get(StormConstants.QSCHEMA_KEY));

    boolean hdfs = (boolean) map.get(StormConstants.USE_HDFS);

    if (hdfs)
    {
      String hdfsUri = (String) map.get(StormConstants.HDFS_URI_KEY);
      FileSystem fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
      LoadDataSchemas.initialize(true, fs);
      LoadQuerySchemas.initialize(true, fs);
    }
    else
    {
      LoadDataSchemas.initialize();
      LoadQuerySchemas.initialize();
    }
  }

  protected static boolean isTickTuple(Tuple tuple)
  {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
  }

}
