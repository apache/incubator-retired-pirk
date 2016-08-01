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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class JavaSerializer extends SerializationService
{

  /**
   * Stores the given object on the given stream using Java serialization.
   * 
   * @param outputStream
   *          The stream on which to store the object.
   * @param obj
   *          The object to be stored.
   * @throws IOException
   *           If a problem occurs storing the object on the given stream.
   */

  public void write(OutputStream outputStream, Storable obj) throws IOException
  {
    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
    oos.writeObject(obj);
  }

  /**
   * Read an object from the given stream of the given type.
   * 
   * @param inputStream
   *          The stream from which to read the object.
   * @param classType
   *          The type of object being retrieved.
   * @throws IOException
   *           If a problem occurs reading the object from the stream.
   */
  @SuppressWarnings("unchecked")
  public <T> T read(InputStream inputStream, Class<T> classType) throws IOException
  {
    try (ObjectInputStream oin = new ObjectInputStream(inputStream))
    {
      return (T) oin.readObject();
    } catch (ClassNotFoundException e)
    {
      throw new RuntimeException(e);
    }
  }
}
