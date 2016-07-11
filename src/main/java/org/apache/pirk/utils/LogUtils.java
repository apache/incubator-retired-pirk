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

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Class for basic logging utils
 */
public class LogUtils
{
  public static Logger logger;

  static
  {
    initialize();
  }

  public static void initialize()
  {
    Properties props = new Properties();
    try
    {
      String log4jFilename = SystemConfiguration.getProperty("log4jPropertiesFile");
      if (log4jFilename == null)
      {
        System.err.println("log4jPropertiesFile property not found during LogUtils initialization.");
      }
      else
      {
        InputStream stream = SystemConfiguration.class.getClassLoader().getResourceAsStream(log4jFilename);
        if (stream != null)
        {
          System.out.println("Loading log4j properties file: '" + log4jFilename + "'");
          props.load(stream);
          PropertyConfigurator.configure(props);
          stream.close();
        }
        else
        {
          System.err.println("log4j properties file not found: '" + log4jFilename + "'");
        }
      }
    } catch (Exception e)
    {
      System.err.println("Exception occured configuring the log4j system: " + e.toString());
      e.printStackTrace();
    }
  }

  /**
   * Should be called at the from from each class using log4j. Example: static private Logger logger = LogUtils.getLoggerForThisClass();
   * 
   * @return
   */
  public static Logger getLoggerForThisClass()
  {
    // We use the third stack element; second is this method, first is
    // .getStackTrace()
    StackTraceElement myCaller = Thread.currentThread().getStackTrace()[2];
    return Logger.getLogger(myCaller.getClassName());
  }

  /**
   * Returns the name of the class calling this method.
   * 
   */
  public static String getNameForThisClass()
  {
    // We use the third stack element; second is this method, first is
    // .getStackTrace()
    StackTraceElement myCaller = Thread.currentThread().getStackTrace()[2];
    return myCaller.getClassName();
  }

  public static String entering()
  {
    StackTraceElement myCaller = Thread.currentThread().getStackTrace()[2];
    String methodName = myCaller.getMethodName();
    return entering(methodName);
  }

  public static String exiting()
  {
    StackTraceElement myCaller = Thread.currentThread().getStackTrace()[2];
    String methodName = myCaller.getMethodName();
    return exiting(methodName);
  }

  public static String entering(String methodName)
  {
    return String.format("Entering %s", methodName);
  }

  public static String exiting(String methodName)
  {
    return String.format("Exiting %s", methodName);
  }
}
