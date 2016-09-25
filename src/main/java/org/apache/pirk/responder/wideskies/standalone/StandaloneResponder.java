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
package org.apache.pirk.responder.wideskies.standalone;

import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.responder.wideskies.spi.ResponderPlugin;
import org.apache.pirk.serialization.LocalFileSystemStore;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class to launch stand alone responder
 */
public class StandaloneResponder implements ResponderPlugin
{
  private static final Logger logger = LoggerFactory.getLogger(StandaloneResponder.class);

  @Override
  public String getPlatformName()
  {
    return "standalone";
  }

  @Override
  public void run()
  {
    logger.info("Launching Standalone Responder:");
    String queryInput = SystemConfiguration.getProperty("pir.queryInput");
    try
    {
      Query query = new LocalFileSystemStore().recall(queryInput, Query.class);
      Responder pirResponder = new Responder(query);
      pirResponder.computeStandaloneResponse();
    } catch (IOException e)
    {
      logger.error("Error reading {}, {}", queryInput, e.getMessage());
    }
  }
}
