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
package org.apache.pirk.serialization;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalFileSystemStore extends StorageService
{
  /**
   * Creates a new storage service on the local file system using default Json serialization.
   */
  public LocalFileSystemStore()
  {
    super();
  }

  /**
   * Creates a new storage service on the local file system using the given serializer.
   */
  public LocalFileSystemStore(SerializationService serial)
  {
    super(serial);
  }

  /**
   * Stores the given object at the given path. The object is serialized using the configured serializer.
   * 
   * @param path
   *          The local file system path.
   * @param obj
   *          The object to store.
   * @throws IOException
   *           If a problem occurs storing the object.
   */
  public void store(String path, Storable obj) throws IOException
  {
    store(new File(path), obj);
  }

  /**
   * Stores the given object at the given file location. The object is serialized using the configured serializer.
   * 
   * @param file
   *          The local file system location to store the object.
   * @param obj
   *          The object to store.
   * @throws IOException
   *           If a problem occurs storing the object.
   */
  public void store(File file, Storable obj) throws IOException
  {
    try (FileOutputStream fos = new FileOutputStream(file))
    {
      serializer.write(fos, obj);
    }
  }

  /**
   * Returns the object stored in the local file system at the given path.
   * 
   * @param path
   *          The local file system path.
   * @param type
   *          The type of object being retrieved.
   * @return The object retrieved from the store.
   * @throws IOException
   *           If a problem occurs retrieving the object.
   */
  public <T> T recall(String path, Class<T> type) throws IOException
  {
    return recall(new File(path), type);
  }

  /**
   * Returns the object stored in the local file system at the given file location.
   * 
   * @param file
   *          The local file system location.
   * @param type
   *          The type of object being retrieved.
   * @return The object retrieved from the store.
   * @throws IOException
   *           If a problem occurs retrieving the object.
   */
  public <T> T recall(File file, Class<T> type) throws IOException
  {
    try (FileInputStream fis = new FileInputStream(file))
    {
      return serializer.read(fis, type);
    }
  }
}
