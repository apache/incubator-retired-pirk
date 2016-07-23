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
package org.apache.pirk.schema.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.pirk.schema.data.partitioner.DataPartitioner;
import org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold a data schema
 */
public class DataSchema implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(DataSchema.class);

  private String schemaName = null;

  private String primitiveTypePartitionerName = null;

  private transient HashMap<String,Text> textRep = null; // string element name -> Text representation

  private transient HashMap<String,Object> partitionerInstances = null; // partitioner class name -> Text representation

  private HashMap<String,String> typeMap = null; // string element name -> java type

  private HashMap<String,String> partitionerMap = null; // string element name -> partitioner class name

  private HashSet<String> listRep = null; // elements that are list/array types

  public DataSchema(String schemaNameInput, HashMap<String,Text> textRepInput, HashSet<String> listRepInput, HashMap<String,String> typeMapInput,
      HashMap<String,String> partitionerMapInput)
  {
    schemaName = schemaNameInput;
    textRep = textRepInput;
    listRep = listRepInput;
    typeMap = typeMapInput;
    partitionerMap = partitionerMapInput;
    primitiveTypePartitionerName = PrimitiveTypePartitioner.class.getName();
  }

  public String getSchemaName()
  {
    return schemaName;
  }

  public HashMap<String,Text> getTextRep()
  {
    if (textRep == null)
    {
      constructTextRep();
    }
    return textRep;
  }

  private void constructTextRep()
  {
    textRep = new HashMap<>();
    for (String name : typeMap.keySet())
    {
      textRep.put(name, new Text(name));
    }
  }

  /**
   * Method to get the partitionerInstances HashMap<String,Object> of partitionerName -> partitionerInstance
   * <p>
   * Will create it if it doesn't already exist
   */
  public HashMap<String,Object> getPartitionerInstances() throws Exception
  {
    if (partitionerInstances == null)
    {
      constructPartitionerInstances();
    }
    return partitionerInstances;
  }

  private void constructPartitionerInstances() throws Exception
  {
    partitionerInstances = new HashMap<>();
    for (String partitionerName : partitionerMap.values())
    {
      if (!partitionerInstances.containsKey(partitionerName))
      {
        if (partitionerName.equals(primitiveTypePartitionerName))
        {
          partitionerInstances.put(primitiveTypePartitionerName, new PrimitiveTypePartitioner());
        }
        else
        // If we have a non-primitive partitioner
        {
          Class c = Class.forName(partitionerName);
          Object obj = c.newInstance();

          // Interface check again just in case the class is used independently of the LoadDataSchemas load functionality
          if (!(obj instanceof DataPartitioner))
          {
            throw new Exception("partitionerName = " + partitionerName + " DOES NOT implement the DataPartitioner interface");
          }
          partitionerInstances.put(partitionerName, obj);
        }
      }
    }
  }

  /**
   * Method to set the partitionerInstances HashMap<String,Object> of partitionerName -> partitionerInstance
   */
  public void setPartitionerInstances(HashMap<String,Object> partitionerInstancesInput)
  {
    partitionerInstances = partitionerInstancesInput;
  }

  /**
   * Method to get the partitioner class instance corresponding to the given partitioner class name
   * <p>
   * Will construct the partitionerInstances HashMap if it doesn't exist
   */
  public Object getPartitionerInstance(String partitionerName) throws Exception
  {
    if (partitionerInstances == null)
    {
      constructPartitionerInstances();
    }
    return partitionerInstances.get(partitionerName);
  }

  /**
   * Method to get the partitioner instance given an element name
   * <p>
   * Will construct the partitionerInstances HashMap if it doesn't exist
   */
  public Object getPartitionerForElement(String element) throws Exception
  {
    return getPartitionerInstance(partitionerMap.get(element));
  }

  /**
   * Method to get the partitioner class name given an element name
   */
  public String getPartitionerName(String element)
  {
    return partitionerMap.get(element);
  }

  /**
   * Get the representation of a given element name
   */
  public Text getTextElement(String element)
  {
    if (textRep == null)
    {
      constructTextRep();
    }

    return textRep.get(element);
  }

  public HashMap<String,String> getTypeMap()
  {
    return typeMap;
  }

  public String getElementType(String element)
  {
    return typeMap.get(element);
  }

  public boolean containsElement(String element)
  {
    return textRep.keySet().contains(element);
  }

  public HashSet<String> getListRep()
  {
    return listRep;
  }

  public HashSet<String> getNonListRep()
  {
    HashSet<String> elements = new HashSet<>();
    elements.addAll(textRep.keySet());
    elements.removeAll(listRep);
    return elements;
  }

  public boolean hasListRep(String element)
  {
    return listRep.contains(element);
  }
}
