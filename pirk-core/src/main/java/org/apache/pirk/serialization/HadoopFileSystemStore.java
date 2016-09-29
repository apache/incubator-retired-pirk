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
package org.apache.pirk.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileSystemStore extends StorageService
{

  private FileSystem hadoopFileSystem;

  // Prevents others from using default constructor.
  HadoopFileSystemStore()
  {
    super();
  }

  /**
   * Creates a new storage service on the given HDFS file system using default Json serialization.
   */
  public HadoopFileSystemStore(FileSystem fs)
  {
    super();
    hadoopFileSystem = fs;
  }

  /**
   * Creates a new storage service on the given HDFS file system using the given serializer
   */
  public HadoopFileSystemStore(FileSystem fs, SerializationService serial)
  {
    super(serial);
    hadoopFileSystem = fs;
  }

  /**
   * Store the given object into the HDFS file system at the given path name.
   * 
   * @param pathName
   *          The location to store the object.
   * @param value
   *          The object to store.
   * @throws IOException
   *           If a problem occurs storing the object.
   */
  public void store(String pathName, Storable value) throws IOException
  {
    store(new Path(pathName), value);
  }

  /**
   * Store the given object at into the HDFS file system at the given path.
   * 
   * @param path
   *          The HDFS path descriptor.
   * @param obj
   *          The object to store.
   * @throws IOException
   *           If a problem occurs storing the object at the given path.
   */
  public void store(Path path, Storable obj) throws IOException
  {
    try (OutputStream os = hadoopFileSystem.create(path))
    {
      serializer.write(os, obj);
    }
  }

  /**
   * Retrieves the object stored at the given path name in HDFS.
   * 
   * @param pathName
   *          The path name where the object is stored.
   * @param type
   *          The type of object being retrieved.
   * @return The object stored at that path name.
   * @throws IOException
   *           If a problem occurs retrieving the object.
   */
  public <T> T recall(String pathName, Class<T> type) throws IOException
  {
    return recall(new Path(pathName), type);
  }

  /**
   * Retrieves the object stored at the given path in HDFS.
   * 
   * @param path
   *          The HDFS path descriptor to the object.
   * @param type
   *          The type of object being retrieved.
   * @return The object stored at that path.
   * @throws IOException
   *           If a problem occurs retrieving the object.
   */
  public <T> T recall(Path path, Class<T> type) throws IOException
  {
    try (InputStream is = hadoopFileSystem.open(path))
    {
      return serializer.read(is, type);
    }
  }
}
