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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class holding basic fileIO utils
 */
public class FileIOUtils
{
  private static final Logger logger = LoggerFactory.getLogger(FileIOUtils.class);

  public interface Callable<V>
  {
    V call(String line) throws Exception;
  }

  public static ArrayList<String> readToArrayList(String filepath)
  {
    return (ArrayList<String>) read(filepath, new ArrayList<>(), new Callable<String>()
    {
      @Override
      public String call(String line)
      {
        return line;
      }
    });
  }

  public static ArrayList<String> readToArrayList(String filepath, Callable<String> function)
  {
    return (ArrayList<String>) read(filepath, new ArrayList<>(), function);
  }

  public static HashSet<String> readToHashSet(String filepath)
  {
    return (HashSet<String>) read(filepath, new HashSet<>(), new Callable<String>()
    {
      @Override
      public String call(String line)
      {
        return line;
      }
    });
  }

  public static HashSet<String> readToHashSet(String filepath, Callable<String> function)
  {
    return (HashSet<String>) read(filepath, new HashSet<>(), function);
  }

  public static AbstractCollection<String> read(String filepath, AbstractCollection<String> collection, Callable<String> function)
  {
    File file = new File(filepath);

    // if file does not exist, output error and return null
    if (!file.exists())
    {
      logger.error("file at " + filepath + " does not exist");
      return null;
    }

    // if file cannot be read, output error and return null
    if (!file.canRead())
    {
      logger.error("cannot read file at " + filepath);
      return null;
    }

    // create buffered reader
    try (BufferedReader br = new BufferedReader(new FileReader(file)))
    {
      // read through the file, line by line
      String line;
      while ((line = br.readLine()) != null)
      {
        String item = function.call(line);
        if (item != null)
        {
          collection.add(item);
        }
      }
    } catch (Exception e)
    {
      logger.error("unable to read file");
    }

    return collection;
  }

  public static void writeArrayList(ArrayList<String> aList, File file) throws IOException
  {
    try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file))))
    {
      for (String s : aList)
      {
        bw.write(s);
        bw.newLine();
      }
    }
  }

  public static void writeArrayList(ArrayList<String> aList, String filename) throws IOException
  {
    writeArrayList(aList, new File(filename));
  }
}
