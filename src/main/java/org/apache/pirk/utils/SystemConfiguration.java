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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages system properties. At first loading of this class, it will:
 * <p>
 * 1) Load in the DEFAULT_PROPERTY_FILE, if found on the classpath. (Currently 'pirk.properties')
 * <p>
 * 2) Load in any properties files in the LOCAL_PROPERTIES_DIR. The filenames must end with '.properties'
 * <p>
 * 3) Load in properties from the QUERIER_PROPERTIES_FILE and RESPONDER_PROPERTIES_FILE
 * 
 */
public class SystemConfiguration
{
  private static final Logger logger = LoggerFactory.getLogger(SystemConfiguration.class);

  private static final Properties props;

  /**
   * By default, these files should be found on the root of the classpath
   */
  private static final String DEFAULT_PROPERTY_FILE = "pirk.properties";
  private static final String QUERIER_PROPERTIES_FILE = "querier.properties";
  private static final String RESPONDER_PROPERTIES_FILE = "responder.properties";

  private static final String LOCAL_PROPERTIES_DIR = "local.pirk.properties.dir";

  static
  {
    props = new Properties();
    initialize();
  }

  public static void initialize()
  {
    // First try to load the default properties file
    loadPropsFromStream(DEFAULT_PROPERTY_FILE);

    // Try to load props from the querier and responder property files, if they exist
    loadPropsFromStream(QUERIER_PROPERTIES_FILE);
    loadPropsFromStream(RESPONDER_PROPERTIES_FILE);

    // Try to load the local properties files, if they exists
    loadPropsFromDir(LOCAL_PROPERTIES_DIR);
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

  public static boolean hasProperty(String propertyName)
  {
    return props.containsKey(propertyName);
  }

  /**
   * Append a property via a comma separated list
   * <p>
   * If the property does not exist, it adds it
   */
  public static void appendProperty(String property, String propToAdd)
  {
    String value = props.getProperty(property);

    if (value != null && !value.equals("none"))
    {
      value += "," + propToAdd;
    }
    else
    {
      value = propToAdd;
    }
    props.setProperty(property, value);
  }

  /**
   * Reset all properties to the default values
   */
  public static void resetProperties()
  {
    clearProperties();
    initialize();
  }

  /**
   * Loads the properties from local properties file in the specified directory
   * <p>
   * Only files ending in '.properties' will be loaded
   */
  public static void loadPropsFromDir(String dirName)
  {
    File[] directoryListing = new File(dirName).listFiles(new FilenameFilter()
    {
      @Override
      public boolean accept(File dir, String name)
      {
        return name.endsWith(".properties");
      }
    });

    if (directoryListing != null)
    {
      for (File file : directoryListing)
      {
        loadPropsFromFile(file);
      }
    }
  }

  /**
   * Loads the properties from the specified file
   */
  public static void loadPropsFromFile(File file)
  {
    if (file.exists())
    {
      try (InputStream stream = new FileInputStream(file);)
      {
        logger.info("Loading properties file '" + file.getAbsolutePath() + "'");
        props.load(stream);
        stream.close();
      } catch (IOException e)
      {
        logger.error("Problem loading properties file '" + file.getAbsolutePath() + "'");
        e.printStackTrace();
      }
    }
    else
    {
      logger.warn("Properties file does not exist: '" + file.getAbsolutePath() + "'");
    }
  }

  /**
   * Loads the properties from the specified file on the classpath
   */
  public static void loadPropsFromStream(String name)
  {
    try
    {
      InputStream stream = SystemConfiguration.class.getClassLoader().getResourceAsStream(name);
      if (stream != null)
      {
        logger.info("Loading file '" + name + "'");
        props.load(stream);
        stream.close();
      }
      else
      {
        logger.error("No file found '" + name + "'");
      }
    } catch (IOException e)
    {
      logger.error("Problem loading file '" + name + "'");
      e.printStackTrace();
    }
  }
}
