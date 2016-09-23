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

import org.apache.pirk.responder.wideskies.spi.ResponderPlugin;
import org.apache.pirk.utils.PIRException;
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

  public static void main(String[] args)
  {
    ResponderCLI responderCLI = new ResponderCLI(args);

    String platformName = SystemConfiguration.getProperty(ResponderProps.PLATFORM, "None");
    logger.info("Attempting to use platform {} ...", platformName);
    try
    {
      ResponderPlugin responder = ResponderService.getInstance().getResponder(platformName);
      if (responder == null)
      {
        logger.error("No such platform plugin found: {}!", platformName);
      }
      else
      {
        responder.run();
      }
    }
    catch (PIRException pirEx)
    {
      logger.error("Failed to load platform plugin: {}! {}", platformName, pirEx.getMessage());
    }
    catch (Exception ex)
    {
      logger.error("Failed to run platform plugin: {}! {}", platformName, ex);
    }
  }
}
