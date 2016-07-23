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
package org.apache.pirk.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.pirk.schema.data.LoadDataSchemas;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages system properties. At first loading of this class, it will:
 * <p>
 * 1) Load in the DEFAULT_PROPERTY_FILE, if found on the classpath. (Currently 'pirk.properties')
 * <p>
 * 2) Load in any properties from LOCAL_PROPERTY_FILE
 * 
 */
public class SystemConfiguration
{
  private static final Logger logger = LoggerFactory.getLogger(SystemConfiguration.class);

  private static final Properties props;

  /**
   * By default, if the pirk.properties file is found on the root of the classpath, it is loaded first.
   */
  private static final String DEFAULT_PROPERTY_FILE = "pirk.properties";

  /**
   * By default, if the local.pirk.properties file is found on the root of the classpath, it is loaded after pirk.properites.
   */
  private static final String LOCAL_PROPERTY_FILE = "local.pirk.properties";

  static
  {
    props = new Properties();
    initialize();

    // Load any data schema files indicated in the properties
    try
    {
      LoadDataSchemas.class.newInstance();
    } catch (Exception e)
    {
      logger.error("Issue when invoking LoadDataSchemas");
      e.printStackTrace();
    }

    // Load any query schema files indicated in the properties
    try
    {
      LoadQuerySchemas.class.newInstance();
    } catch (Exception e)
    {
      logger.error("Issue when invoking LoadDataSchemas");
      e.printStackTrace();
    }
  }

  public static void initialize()
  {
    // First try to load the default properties file
    try
    {
      InputStream stream = SystemConfiguration.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTY_FILE);
      if (stream != null)
      {
        logger.info("Loading default properties file '" + DEFAULT_PROPERTY_FILE + "'");
        props.load(stream);
        stream.close();
      }
      else
      {
        logger.error("No default configuration file found '" + DEFAULT_PROPERTY_FILE + "'");
      }
    } catch (IOException e)
    {
      logger.error("Problem loading default properties file '" + DEFAULT_PROPERTY_FILE + "'");
      e.printStackTrace();
    }

    // Try to load the local properties file, if one exists
    File localFile = new File(getProperty(LOCAL_PROPERTY_FILE));
    if (localFile.exists())
    {
      try(InputStream stream = new FileInputStream(localFile);)
      {
        logger.info("Loading local properties file '" + localFile.getAbsolutePath() + "'");
        props.load(stream);
        stream.close();
      } catch (IOException e)
      {
        logger.error("Problem loading local properties file '" + localFile.getAbsolutePath() + "'");
        e.printStackTrace();
      }
    }
  }

  /**
   * Clear the properties
   */
  public static void clearProperties()
  {
    props.clear();
  }

  /**
   * Gets the specified property; returns null if the property isn't found.
   * 
   */
  public static String getProperty(String propertyName)
  {
    return props.getProperty(propertyName);
  }

  /**
   * Gets the specified property; returns the defaultValue if the property isn't found.
   * 
   */
  public static String getProperty(String propertyName, String defaultValue)
  {
    return props.getProperty(propertyName, defaultValue);
  }

  /**
   * Set a property
   */
  public static void setProperty(String propertyName, String value)
  {
    props.setProperty(propertyName, value);
  }

  /**
   * Reset all properties to the default values
   */
  public static void resetProperties()
  {
    clearProperties();
    initialize();
  }
}
