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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for basic HDFS fileIO utils
 */
public class HDFS
{
  private static final Logger logger = LoggerFactory.getLogger(HDFS.class);

  public static void writeFile(Collection<String> elements, FileSystem fs, String path, boolean deleteOnExit)
  {
    Path filePath = new Path(path);

    try
    {
      // create writer
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(filePath, true)));

      // write each element on a new line
      for (String element : elements)
      {
        bw.write(element);
        bw.newLine();
      }

      bw.close();

      // delete file once the filesystem is closed
      if (deleteOnExit)
      {
        fs.deleteOnExit(filePath);
      }

    } catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  public static void writeFile(List<JSONObject> elements, FileSystem fs, String path, boolean deleteOnExit)
  {
    Path filePath = new Path(path);

    try
    {
      // create writer
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(filePath, true)));

      // write each element on a new line
      for (JSONObject element : elements)
      {
        bw.write(element.toString());
        bw.newLine();
      }
      bw.close();

      // delete file once the filesystem is closed
      if (deleteOnExit)
      {
        fs.deleteOnExit(filePath);
      }
    } catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  public static void writeFile(List<BigInteger> elements, FileSystem fs, Path path, boolean deleteOnExit)
  {
    try
    {
      // create writer
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));

      // write each element on a new line
      for (BigInteger element : elements)
      {
        bw.write(element.toString());
        bw.newLine();
      }
      bw.close();

      // delete file once the filesystem is closed
      if (deleteOnExit)
      {
        fs.deleteOnExit(path);
      }
    } catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  public static void writeFile(Map<String,Integer> sortedMap, FileSystem fs, String path, boolean deleteOnExit)
  {
    Path filePath = new Path(path);

    try
    {
      // create writer
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(filePath, true)));

      // write each element on a new line
      for (Entry<String,Integer> entry : sortedMap.entrySet())
      {
        bw.write(entry.getKey() + "," + entry.getValue());
        bw.newLine();
      }
      bw.close();

      // delete file once the filesystem is closed
      if (deleteOnExit)
      {
        fs.deleteOnExit(filePath);
      }

    } catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  public static ArrayList<String> readFile(FileSystem fs, String filepath)
  {
    Path path = new Path(filepath);
    return readFile(fs, path);
  }

  public static ArrayList<String> readFile(FileSystem fs, Path path)
  {
    ArrayList<String> rv = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path))))
    {
      String line;
      while ((line = br.readLine()) != null)
      {
        rv.add(line);
      }
    } catch (Exception e)
    {
      e.printStackTrace();
      return null;
    }
    return rv;
  }

  public static HashSet<String> readFileHashSet(FileSystem fs, Path path)
  {
    HashSet<String> rv = new HashSet<>();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path))))
    {
      String line;
      while ((line = br.readLine()) != null)
      {
        rv.add(line);
      }

    } catch (Exception e)
    {
      e.printStackTrace();
      return null;
    }

    return rv;
  }

  public static ArrayList<Path> getFilesInDirectory(FileSystem fs, String directory)
  {

    ArrayList<Path> files = new ArrayList<>();

    try
    {

      Path path = new Path(directory);
      FileStatus[] statuses = fs.listStatus(path);

      if (statuses != null)
      {
        for (FileStatus status : statuses)
        {
          files.add(status.getPath());
        }
      }

    } catch (IOException e)
    {
      e.printStackTrace();
      return null;
    }

    return files;

  }

  public static Path findFileInDirectory(FileSystem fs, String directory, String startsWith)
  {

    ArrayList<Path> files = getFilesInDirectory(fs, directory);

    if (files == null)
    {
      return null;
    }

    for (Path file : files)
    {
      if (file.getName().startsWith(startsWith))
      {
        return file;
      }
    }

    return null;
  }

  public static void deletePath(FileSystem fs, String filepath)
  {
    Path path = new Path(filepath);

    try
    {
      fs.delete(path, true);
    } catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  public static void writeFileIntegers(List<Integer> elements, FileSystem fs, Path path, boolean deleteOnExit)
  {
    try
    {
      // create writer
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));

      // write each element on a new line
      for (Integer element : elements)
      {
        bw.write(element.toString());
        bw.newLine();
      }
      bw.close();

      // delete file once the filesystem is closed
      if (deleteOnExit)
      {
        fs.deleteOnExit(path);
      }
    } catch (IOException e)
    {
      e.printStackTrace();
    }

  }

}
