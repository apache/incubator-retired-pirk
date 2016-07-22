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
   * Creates a new storage service on the given HDFS file system using default Java serialization.
   */
  public HadoopFileSystemStore(FileSystem fs)
  {
    super();
    hadoopFileSystem = fs;
  }

  public HadoopFileSystemStore(FileSystem fs, SerializationService serial)
  {
    super(serial);
    hadoopFileSystem = fs;
  }

  public void store(String pathName, Storable value) throws IOException
  {
    store(new Path(pathName), value);
  }

  public void store(Path path, Storable obj) throws IOException
  {
    OutputStream os = hadoopFileSystem.create(path);
    try
    {
      serializer.write(os, obj);
    } finally
    {
      if (os != null)
      {
        os.close();
      }
    }
  }

  public <T> T recall(String pathName, Class<T> type) throws IOException
  {
    return recall(new Path(pathName), type);
  }

  public <T> T recall(Path path, Class<T> type) throws IOException
  {
    InputStream is = hadoopFileSystem.open(path);
    try
    {
      return serializer.read(is, type);
    } finally
    {
      if (is != null)
      {
        is.close();
      }
    }
  }

}
