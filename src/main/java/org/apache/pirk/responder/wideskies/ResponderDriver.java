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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;

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
  // ClassNames to instantiate Platforms using the platform CLI
  private final static String MAPREDUCE_LAUNCHER = "org.apache.pirk.responder.wideskies.mapreduce.MapReduceResponderLauncher";
  private final static String SPARK_LAUNCHER = "org.apache.pirk.responder.wideskies.spark.SparkResponderLauncher";
  private final static String SPARKSTREAMING_LAUNCHER = "org.apache.pirk.responder.wideskies.spark.streaming.SparkStreamingResponderLauncher";
  private final static String STANDALONE_LAUNCHER = "org.apache.pirk.responder.wideskies.standalone.StandaloneResponderLauncher";
  private final static String STORM_LAUNCHER = "org.apache.pirk.responder.wideskies.storm.StormResponderLauncher";

  private enum Platform
  {
    MAPREDUCE, SPARK, SPARKSTREAMING, STORM, STANDALONE, NONE
  }

  private static void launch(String launcherClassName)
  {
    logger.info("Launching Responder with {}", launcherClassName);
    try
    {
      Class clazz = Class.forName(launcherClassName);
      if (ResponderLauncher.class.isAssignableFrom(clazz))
      {
        Object launcherInstance = clazz.newInstance();
        Method m = launcherInstance.getClass().getDeclaredMethod("run");
        m.invoke(launcherInstance);
      }
      else
      {
        logger.error("Class {} does not implement ResponderLauncher", launcherClassName);
      }
    }
    catch (ClassNotFoundException e)
    {
      logger.error("Class {} not found, check launcher property: {}", launcherClassName);
    }
    catch (NoSuchMethodException e)
    {
      logger.error("In {} run method not found: {}", launcherClassName);
    }
    catch (InvocationTargetException e)
    {
      logger.error("In {} run method could not be invoked: {}: {}", launcherClassName, e);
    }
    catch (InstantiationException e)
    {
      logger.error("Instantiation exception within {}: {}", launcherClassName, e);
    }
    catch (IllegalAccessException e)
    {
      logger.error("IllegalAccess Exception {}", e);
    }
  }

  public static void main(String[] args)
  {
    ResponderCLI responderCLI = new ResponderCLI(args);

    // For handling System.exit calls from Spark Streaming
    System.setSecurityManager(new SystemExitManager());

    String launcherClassName = SystemConfiguration.getProperty(ResponderProps.LAUNCHER);
    if (launcherClassName != null)
    {
      launch(launcherClassName);
    }
    else
    {
      logger.warn("platform is being deprecaited in flavor of launcher");
      Platform platform = Platform.NONE;
      String platformString = SystemConfiguration.getProperty(ResponderProps.PLATFORM);

      try
      {
        platform = Platform.valueOf(platformString.toUpperCase());
        logger.info("platform = " + platform);
      } catch (IllegalArgumentException e)
      {
        logger.error("platform " + platformString + " not found.");
      }

      switch (platform)
      {
        case MAPREDUCE:
          launch(MAPREDUCE_LAUNCHER);
          break;

        case SPARK:
          launch(SPARK_LAUNCHER);
          break;

        case SPARKSTREAMING:
          launch(SPARKSTREAMING_LAUNCHER);
          break;

        case STORM:
          launch(STORM_LAUNCHER);
          break;

        case STANDALONE:
          launch(STANDALONE_LAUNCHER);
          break;
      }
    }
  }

  // Exception and Security Manager classes used to catch System.exit from Spark Streaming
  public static class SystemExitException extends SecurityException
  {}

  private static class SystemExitManager extends SecurityManager
  {
    @Override
    public void checkPermission(Permission perm)
    {}

    @Override
    public void checkExit(int status)
    {
      super.checkExit(status);
      if (status == 0) // If we exited cleanly, throw SystemExitException
      {
        throw new SystemExitException();
      }
      else
      {
        throw new SecurityException();
      }

    }
  }
}
