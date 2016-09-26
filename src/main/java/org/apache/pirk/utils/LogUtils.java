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
package org.apache.pirk.utils;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.log4j.PropertyConfigurator;

/**
 * Class to update logging in new environments like hadoop or yarn.
 * Useful for when you want to change the log levels in hadoop or yarn, which
 * by default would otherwise ignore our logging settings.
 */
public class LogUtils
{

  private static final Logger logger = LoggerFactory.getLogger(LogUtils.class);

//  static
//  {
//    reassertLogProperties();
//  }

  public static void reassertLogProperties()
  {
    Properties props = new Properties();
    try
    {
      String log4jFilename = SystemConfiguration.getProperty("log4jPropertiesFile");
      if (log4jFilename == null)
      {
        logger.error("log4jPropertiesFile property not found during LogUtils initialization.");
      }
      else
      {
        InputStream stream = SystemConfiguration.class.getClassLoader().getResourceAsStream(log4jFilename);
        if (stream != null)
        {
          logger.info("Loading log4j properties file: '" + log4jFilename + "'");
          props.load(stream);
          PropertyConfigurator.configure(props);
          stream.close();
        }
        else
        {
          logger.info("log4j properties file not found: '" + log4jFilename + "'");
        }
      }
    } catch (Exception e)
    {
      logger.error("Exception occured configuring the log4j system: " + e.toString());
      e.printStackTrace();
    }
  }
}
