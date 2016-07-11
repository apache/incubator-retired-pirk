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
package org.apache.pirk.response.wideskies;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.utils.LogUtils;

/**
 * Class to hold the encrypted response elements for the PIR query
 * <p>
 * Serialized and returned to the querier for decryption
 * 
 */
public class Response implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static Logger logger = LogUtils.getLoggerForThisClass();

  QueryInfo queryInfo = null; // holds all query info

  TreeMap<Integer,BigInteger> responseElements = null; // encrypted response columns, colNum -> column

  public Response(QueryInfo queryInfoInput)
  {
    queryInfo = queryInfoInput;

    responseElements = new TreeMap<Integer,BigInteger>();
  }

  public TreeMap<Integer,BigInteger> getResponseElements()
  {
    return responseElements;
  }

  public void setResponseElements(TreeMap<Integer,BigInteger> elements)
  {
    responseElements = elements;
  }

  public QueryInfo getQueryInfo()
  {
    return queryInfo;
  }

  public void addElement(int position, BigInteger element)
  {
    responseElements.put(position, element);
  }

  public void writeToFile(String filename) throws IOException
  {
    writeToFile(new File(filename));
  }

  public void writeToFile(File file) throws IOException
  {
    ObjectOutputStream oos = null;
    FileOutputStream fout = null;
    try
    {
      fout = new FileOutputStream(file, true);
      oos = new ObjectOutputStream(fout);
      oos.writeObject(this);
    } catch (Exception ex)
    {
      ex.printStackTrace();
    } finally
    {
      if (oos != null)
      {
        oos.close();
      }
      if (fout != null)
      {
        fout.close();
      }
    }
  }

  public void writeToHDFSFile(Path fileName, FileSystem fs)
  {

    ObjectOutputStream oos = null;
    try
    {
      oos = new ObjectOutputStream(fs.create(fileName));
      oos.writeObject(this);
      oos.close();
    } catch (IOException e)
    {
      e.printStackTrace();
    } finally
    {
      if (oos != null)
      {
        try
        {
          oos.close();
        } catch (IOException e)
        {
          e.printStackTrace();
        }
      }
    }
  }

  public static Response readFromFile(String filename) throws IOException
  {
    return readFromFile(new File(filename));
  }

  public static Response readFromFile(File file) throws IOException
  {
    Response response = null;

    ObjectInputStream objectinputstream = null;
    FileInputStream streamIn = null;
    try
    {
      streamIn = new FileInputStream(file);
      objectinputstream = new ObjectInputStream(streamIn);
      response = (Response) objectinputstream.readObject();

    } catch (Exception e)
    {
      e.printStackTrace();
    } finally
    {
      if (objectinputstream != null)
      {
        objectinputstream.close();
      }
      if (streamIn != null)
      {
        streamIn.close();
      }
    }

    return response;
  }

  // Used for testing
  public static Response readFromHDFSFile(Path file, FileSystem fs) throws IOException
  {
    Response response = null;

    ObjectInputStream ois = null;
    try
    {
      ois = new ObjectInputStream(fs.open(file));
      response = (Response) ois.readObject();
      ois.close();

    } catch (IOException | ClassNotFoundException e1)
    {
      e1.printStackTrace();
    } finally
    {
      if (ois != null)
      {
        try
        {
          ois.close();
        } catch (IOException e)
        {
          e.printStackTrace();
        }
      }
    }
    return response;
  }
}
