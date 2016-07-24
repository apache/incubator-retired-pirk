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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalFileSystemStore extends StorageService
{

  /**
   * Creates a new storage service on the local file system using default Java serialization.
   */
  public LocalFileSystemStore()
  {
    super();
  }

  public LocalFileSystemStore(SerializationService serial)
  {
    super(serial);
  }

  public void store(String path, Storable obj) throws IOException
  {
    store(new File(path), obj);
  }

  public void store(File file, Storable obj) throws IOException
  {
    FileOutputStream fos = new FileOutputStream(file);
    try
    {
      serializer.write(fos, obj);
    } finally
    {
      if (fos != null)
      {
        fos.close();
      }
    }
  }

  public <T> T recall(String path, Class<T> type) throws IOException
  {
    return recall(new File(path), type);
  }

  public <T> T recall(File file, Class<T> type) throws IOException
  {
    FileInputStream fis = new FileInputStream(file);
    try
    {
      return serializer.read(fis, type);
    } finally
    {
      if (fis != null)
      {
        fis.close();
      }
    }
  }

}
