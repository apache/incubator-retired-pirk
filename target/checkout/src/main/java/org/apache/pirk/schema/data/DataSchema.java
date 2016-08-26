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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.pirk.schema.data.partitioner.DataPartitioner;
import org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner;
import org.apache.pirk.utils.PIRException;

/**
 * A data schema describes the target data being referenced by a <code>Querier</code> and a <code>Responder</code>.
 * <p>
 * The schema comprises a number of elements, each of which has a name, type, and a partitioner. Elements may be declared as arrays of types.
 * <p>
 * Schemas are typically loaded from XML descriptors.
 * 
 * @see DataSchemaLoader
 */
public class DataSchema implements Serializable
{
  private static final long serialVersionUID = 1L;

  // This schema's name.
  private final String schemaName;

  // Maps element name -> Java type name
  private final Map<String,String> typeMap = new HashMap<>();

  // Maps element name -> partitioner class name.
  private final Map<String,String> partitionerTypeMap = new HashMap<>();

  // Element names that are declared as array types.
  private final Set<String> arrayElements = new HashSet<>();

  // Lazily maps partitioner class name -> an instance of the partitioner.
  private transient Map<String,DataPartitioner> partitionerInstances = new HashMap<>();

  // Lazily maps element name -> Hadoop Text representation.
  private transient Map<String,Text> textRep = new HashMap<>();

  /*
   * Creates an empty, named data schema.
   */
  DataSchema(String schemaName)
  {
    this.schemaName = schemaName;
  }

  /**
   * Returns true if the data schema contains an element with the given name.
   * 
   * @param elementName
   *          The element name to check.
   * @return true if the schema does define an element with that name, of false otherwise.
   */
  public boolean containsElement(String elementName)
  {
    return typeMap.containsKey(elementName);
  }

  /**
   * Returns the set of element names defined by this schema.
   *
   * @return The possibly empty set of element names.
   */
  public Set<String> getElementNames()
  {
    return typeMap.keySet();
  }

  /**
   * Returns the name of the Java type associated with the given element name.
   * <p>
   * The Java type is either a primitive type name, as defined in the {@link PrimitiveTypePartitioner}, or a full canonical class name representing the element
   * type.
   * 
   * @see PrimitiveTypePartitioner
   * @param elementName
   *          The element name whose type is requested.
   * @return The type of the element, or <code>null</code> if the schema does not define the given element name.
   */
  public String getElementType(String elementName)
  {
    return typeMap.get(elementName);
  }

  /**
   * Returns the element names that are declared as arrays.
   *
   * @return The set of names that are arrays, or an empty set if none.
   */
  public Set<String> getArrayElements()
  {
    return arrayElements;
  }

  /**
   * Returns the element names that are declared to not be arrays.
   * 
   * @return The set of names that are not arrays, or an empty set if none.
   */
  public Set<String> getNonArrayElements()
  {
    Set<String> elements = new HashSet<>();
    elements.addAll(typeMap.keySet());
    elements.removeAll(getArrayElements());
    return elements;

  }

  /**
   * Returns the partitioner instance for the given element name.
   * <p>
   * A partitioner for the named type is created on first request, and the same partitioner is returned on subsequent calls.
   * 
   * @param elementName
   *          the name of the element whose partitioner is required.
   * @return the data partitioner, or <code>null</code> if the element does not exist.
   * @throws PIRExcpetion
   *           if the partitioner cannot be instantiated.
   * @see DataSchema#getPartitionerInstance(String)
   */
  public DataPartitioner getPartitionerForElement(String elementName) throws PIRException
  {
    String partitionerType = partitionerTypeMap.get(elementName);
    return partitionerType == null ? null : getPartitionerInstance(partitionerType);
  }

  /**
   * Returns the partitioner corresponding to the given partitioner class name.
   * <p>
   * A partitioner for the named type is created on first request, and the same partitioner is returned on subsequent calls to this method.
   * 
   * @param partitionerTypeName
   *          The class name for a partitioner type.
   * @return The partitioner instance of the requested type.
   * @throws PIRException
   *           If a problem occurs instantiating a new partitioner of the requested type.
   */
  public DataPartitioner getPartitionerInstance(String partitionerTypeName) throws PIRException
  {
    DataPartitioner partitioner = partitionerInstances.get(partitionerTypeName);
    if (partitioner == null)
    {
      boolean isPrimitivePartitioner = partitionerTypeName.equals(PrimitiveTypePartitioner.class.getName());
      partitioner = isPrimitivePartitioner ? new PrimitiveTypePartitioner() : instantiatePartitioner(partitionerTypeName);
      partitionerInstances.put(partitionerTypeName, partitioner);
    }
    return partitioner;
  }

  /**
   * Returns the partitioner type name for a given element name.
   * <p>
   * The partitioner type name is either that of the primitive partitioner, where the element name is a primitive type. For non-primitives it is the fully
   * qualified name of a Java class that implements the {@link DataPartitioner} interface.
   * 
   * @param elementName
   *          The element name whose partitioner type is requested.
   * @return The type name of the element's partitioner, or <code>null</code> if there is no element of that name.
   */
  public String getPartitionerTypeName(String elementName)
  {
    return partitionerTypeMap.get(elementName);
  }

  /**
   * Returns the name of this schema.
   * 
   * @return The schema name.
   */
  public String getSchemaName()
  {
    return schemaName;
  }

  /**
   * Returns the Hadoop text representation of a given element name.
   * 
   * @param elementName
   *          The name of the element whose text representation is requested.
   * @returns The text representation, or <code>null</code> if the element name does not exist in this schema.
   */
  public Text getTextName(String elementName)
  {
    Text text = textRep.get(elementName);
    if (text == null && containsElement(elementName))
    {
      text = new Text(elementName);
      textRep.put(elementName, text);
    }
    return text;
  }

  /**
   * Returns true if the given element name is an array type.
   * <p>
   * The method returns <code>false</code> if the element is not an array type or the schema does not define an element of this type.
   * 
   * @param element
   *          The name of the element to test.
   * @return <code>true</code> if the element is an array type, and <code>false</code> otherwise.
   */
  public boolean isArrayElement(String element)
  {
    return arrayElements.contains(element);
  }

  /*
   * Returns the map of partitionerTypeName -> partitionerInstance
   */
  Map<String,DataPartitioner> getPartitionerInstances()
  {
    return partitionerInstances;
  }

  /*
   * Returns the mapping from element name to partitioner type name.
   */
  Map<String,String> getPartitionerTypeMap()
  {
    return partitionerTypeMap;
  }

  /*
   * Returns the Hadoop text map.
   */
  Map<String,Text> getTextRep()
  {
    return textRep;
  }

  /*
   * Returns the mapping from element name to element's Java type.
   */
  Map<String,String> getTypeMap()
  {
    return typeMap;
  }

  /*
   * Creates a new instance of the partitioner with the given type name, or throws a PIRExcpetion describing the problem.
   */
  DataPartitioner instantiatePartitioner(String partitionerTypeName) throws PIRException
  {
    Object obj;
    try
    {
      @SuppressWarnings("unchecked")
      Class<? extends DataPartitioner> c = (Class<? extends DataPartitioner>) Class.forName(partitionerTypeName);
      obj = c.newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | ClassCastException e)
    {
      throw new PIRException("partitioner = " + partitionerTypeName + " cannot be instantiated or does not implement DataParitioner.", e);
    }

    return (DataPartitioner) obj;
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();

    // Initialize transient elements
    partitionerInstances = new HashMap<>();
    textRep = new HashMap<>();
  }
}
