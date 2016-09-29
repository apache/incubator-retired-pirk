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
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

  private static final Properties props = new Properties();

  /**
   * By default, these files should be found on the root of the classpath
   */
  private static final String DEFAULT_PROPERTY_FILE = "pirk.properties";
  private static final String QUERIER_PROPERTIES_FILE = "querier.properties";
  private static final String RESPONDER_PROPERTIES_FILE = "responder.properties";

  private static final String LOCAL_PROPERTIES_DIR = "local.pirk.properties.dir";

  static
  {
    initialize();
  }

  public static void initialize()
  {
    props.clear();

    // First try to load the default properties file
    loadPropsFromResource(DEFAULT_PROPERTY_FILE);

    // Try to load props from the querier and responder property files, if they exist
    loadPropsFromResource(QUERIER_PROPERTIES_FILE);
    loadPropsFromResource(RESPONDER_PROPERTIES_FILE);

    // Try to load the local properties files, if they exists
    loadPropsFromDir(getProperty(LOCAL_PROPERTIES_DIR));
  }

  /**
   * Return the Properties object maintained by this class.
   *
   * @return The system properties.
   */
  public static Properties getProperties() {
    return props;
  }

  /**
   * Gets the specified property; returns <code>null</code> if the property isn't found.
   * 
   * @param propertyName
   *          The name of the requested property.
   * @return The value of the property, or <code>null</code> if the property cannot be found.
   */
  public static String getProperty(String propertyName)
  {
    return props.getProperty(propertyName);
  }

  /**
   * Gets the specified property as a <code>String</code>, or the default value if the property isn't found.
   * 
   * @param propertyName
   *          The name of the requested string property value.
   * @param defaultValue
   *          The value to return if the property is undefined.
   * @return The value of the requested property, or the default value if the property is undefined.
   */
  public static String getProperty(String propertyName, String defaultValue)
  {
    return props.getProperty(propertyName, defaultValue);
  }

  /**
   * Gets the specified property as an <code>int</code>, or the default value if the property isn't found.
   * 
   * @param propertyName
   *          The name of the requested int property value.
   * @param defaultValue
   *          The value to return if the property is undefined.
   * @return The value of the requested property, or the default value if the property is undefined.
   * @throws NumberFormatException
   *           If the property does not contain a parsable <code>int</code> value.
   */
  public static int getIntProperty(String propertyName, int defaultValue)
  {
    String value = props.getProperty(propertyName);
    return (value == null) ? defaultValue : Integer.parseInt(value);
  }

  /**
   * Gets the specified property as an <code>long</code>, or the default value if the property isn't found.
   * 
   * @param propertyName
   *          The name of the requested long property value.
   * @param defaultValue
   *          The value to return if the property is undefined.
   * @return The value of the requested property, or the default value if the property is undefined.
   * @throws NumberFormatException
   *           If the property does not contain a parsable <code>long</code> value.
   */
  public static long getLongProperty(String propertyName, long defaultValue)
  {
    String value = props.getProperty(propertyName);
    return (value == null) ? defaultValue : Long.parseLong(value);
  }

  /**
   * Gets the specified property as a <code>boolean</code>, or the default value if the property isn't defined.
   * 
   * @param propertyName
   *          The name of the requested boolean property value.
   * @param defaultValue
   *          The value to return if the property is undefined.
   * @return <code>true</code> if the property is defined and has the value "true", otherwise <code>defaultValue</code>.
   */
  public static boolean getBooleanProperty(String propertyName, boolean defaultValue)
  {
    return (isSetTrue(propertyName)) || defaultValue;
  }

  /**
   * Returns <code>true</code> iff the specified boolean property value is "true".
   * <p>
   * If the property is not found, or it's value is not "true" then the method will return <code>false</code>.
   * 
   * @param propertyName
   *          The name of the requested boolean property value.
   * @return <code>true</code> if the property is defined and has the value "true", otherwise <code>false</code>.
   */
  public static boolean isSetTrue(String propertyName)
  {
    String value = props.getProperty(propertyName);
    return "true".equals(value);
  }

  /**
   * Sets the property to the given value.
   * <p>
   * Any previous values stored at the same property name are replaced.
   * 
   * @param propertyName
   *          The name of the property to set.
   * @param value
   *          The property value.
   */
  public static void setProperty(String propertyName, String value)
  {
    props.setProperty(propertyName, value);
  }

  /**
   * Returns true iff the given property name is defined.
   * 
   * @param propertyName
   *          The property name to test.
   * @return <code>true</code> if the property is found in the configuration, or <code>false</code> otherwise.
   */
  public static boolean hasProperty(String propertyName)
  {
    return props.containsKey(propertyName);
  }

  /**
   * Appends a property via a comma separated list
   * <p>
   * If the property does not exist, it adds it.
   * 
   * @param propertyName
   *          The property whose value is to be appended with the given value.
   * @param value
   *          The value to be stored, or appended to the current value.
   */
  public static void appendProperty(String propertyName, String value)
  {
    String oldValue = props.getProperty(propertyName);

    if (oldValue != null && !oldValue.equals("none"))
    {
      oldValue += "," + value;
    }
    else
    {
      oldValue = value;
    }
    props.setProperty(propertyName, oldValue);
  }

  /**
   * Loads the properties from local properties file in the specified directory.
   * <p>
   * All files ending in '.properties' will be loaded. The new properties are added to the current system configuration.
   * 
   * @param dirName
   *          The directory to search for the new properties files.
   */
  public static void loadPropsFromDir(String dirName)
  {
    logger.info("Loading properties from dirName = " + dirName);
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
   * Loads the properties from local properties file in the specified directory in hdfs.
   * <p>
   * All files ending in '.properties' will be loaded. The new properties are added to the current system configuration.
   * 
   * @param dirName
   *          The directory to search for the new properties files.
   * @throws IOException
   * @throws FileNotFoundException
   */
  public static void loadPropsFromHDFSDir(String dirName, FileSystem fs) throws FileNotFoundException, IOException
  {
    logger.info("Loading properties from dirName = " + dirName);

    Path dirPath = new Path(dirName);

    FileStatus[] status = fs.listStatus(dirPath);
    for (int i = 0; i < status.length; i++)
    {
      if (status[i].getPath().getName().endsWith(".properties"))
      {
        loadPropsFromFile(status[i].getPath(), fs);
      }
    }
  }

  /**
   * Loads the properties from the specified file.
   * <p>
   * The new properties are added to the current system configuration.
   * 
   * @param file
   *          The properties file containing the system properties to add.
   */
  public static void loadPropsFromFile(File file)
  {
    if (file.exists())
    {
      try (InputStream stream = new FileInputStream(file))
      {
        logger.info("Loading properties file '" + file.getAbsolutePath() + "'");
        loadProperties(stream);
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
   * Loads the properties from the specified file in hdfs
   * <p>
   * The new properties are added to the current system configuration.
   * 
   * @param file
   *          The properties file containing the system properties to add.
   * @throws IOException
   */
  public static void loadPropsFromFile(String filename, FileSystem fs) throws IOException
  {
    Path p = new Path(filename);
    loadPropsFromFile(p, fs);
  }

  /**
   * Loads the properties from the specified file in hdfs
   * <p>
   * The new properties are added to the current system configuration.
   * 
   * @param file
   *          The properties file containing the system properties to add.
   * @throws IOException
   */
  public static void loadPropsFromFile(Path filePath, FileSystem fs) throws IOException
  {
    if (fs.exists(filePath))
    {
      try (InputStream stream = fs.open(filePath);)
      {
        logger.info("Loading properties file from hdfs'" + filePath.toString() + "'");
        loadProperties(stream);
      } catch (IOException e)
      {
        logger.error("Problem loading properties file from hdfs '" + filePath.toString() + "'");
        e.printStackTrace();
      }
    }
    else
    {
      logger.warn("Properties file does not exist: '" + filePath.toString() + "'");
    }
  }

  /**
   * Loads the properties from the specified resource on the current classloader.
   * <p>
   * The new properties are added to the current system configuration.
   * 
   * @param name
   *          The name of the resource defining the properties.
   */
  public static void loadPropsFromResource(String name)
  {
    try (InputStream stream = SystemConfiguration.class.getClassLoader().getResourceAsStream(name))
    {
      if (stream != null)
      {
        logger.info("Loading file '" + name + "'");
        loadProperties(stream);
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

  /**
   * Load the properties in the Properties object and then trim any whitespace
   * <p>
   * Properties.load does not do this automatically
   * 
   * @throws IOException
   */
  public static void loadProperties(InputStream stream) throws IOException
  {
    props.load(stream);

    Enumeration propKeys = props.propertyNames();
    while (propKeys.hasMoreElements())
    {
      String tmpKey = (String) propKeys.nextElement();
      String tmpValue = props.getProperty(tmpKey);
      tmpValue = tmpValue.trim();
      props.put(tmpKey, tmpValue);
    }
  }
}
