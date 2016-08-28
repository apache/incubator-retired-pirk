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
package org.apache.pirk.responder.wideskies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.responder.wideskies.mapreduce.ComputeResponseTool;
import org.apache.pirk.responder.wideskies.spark.ComputeResponse;
import org.apache.pirk.responder.wideskies.standalone.Responder;
import org.apache.pirk.responder.wideskies.storm.PirkTopology;
import org.apache.pirk.serialization.LocalFileSystemStore;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver class for the responder
 * <p>
 * Pivots on the platform given
 * <p>
 * If mapreduce, kick off with 'hadoop jar' command.
 * <p>
 * If spark, kick off with 'spark-submit' command or integrate into other codeflows with SparkLauncher
 * <p>
 * If standalone, assumes that the target data is in the local filesystem in JSON format with one JSON record per line
 * 
 */
public class ResponderDriver
{
  private static final Logger logger = LoggerFactory.getLogger(ResponderDriver.class);

  enum Platform
  {
    MAPREDUCE, SPARK, STORM, STANDALONE, NONE;
  }

  public static void main(String[] args) throws Exception
  {
    Platform platform = Platform.NONE;
    try
    {
      platform = Platform.valueOf(SystemConfiguration.getProperty(ResponderProps.PLATFORM).toUpperCase());
    } catch (IllegalArgumentException e)
    {
      logger.error("Platform " + platform + " not found");
    }

    switch (platform)
    {
      case MAPREDUCE:
        logger.info("Launching MapReduce ResponderTool:");

        ComputeResponseTool pirWLTool = new ComputeResponseTool();
        ToolRunner.run(pirWLTool, new String[] {});
        break;

      case SPARK:
        logger.info("Launching Spark ComputeResponse:");

        FileSystem fs = FileSystem.get(new Configuration());
        ComputeResponse computeResponse = new ComputeResponse(fs);
        computeResponse.performQuery();
        break;

      case STORM:
        logger.info("Launching Storm PirkTopology:");
        PirkTopology.runPirkTopology();
        break;

      case STANDALONE:
        logger.info("Launching Standalone Responder:");

        String queryInput = SystemConfiguration.getProperty("pir.queryInput");
        Query query = new LocalFileSystemStore().recall(queryInput, Query.class);

        Responder pirResponder = new Responder(query);
        pirResponder.computeStandaloneResponse();
    }
  }
}
