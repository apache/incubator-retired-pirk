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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The data schema registry is a global location for data schema descriptors.
 *
 * @see DataSchema
 * @see DataSchemaLoader
 */
public class DataSchemaRegistry
{
  // The registry. Maps schema name to data schema.
  private static final Map<String,DataSchema> registry = new HashMap<>();

  // Not designed to be instantiated.
  DataSchemaRegistry()
  {

  }

  /**
   * Adds the given data schema to the registry.
   * 
   * If there was an existing schema with the same name, it is replaced.
   * 
   * @param schema
   *          The data schema to add.
   * @return the previous schema registered at the same name, or <code>null</code> if there were none.
   */
  public static DataSchema put(DataSchema schema)
  {
    return registry.put(schema.getSchemaName(), schema);
  }

  /**
   * Returns the data schema with the given name.
   * 
   * @param schemaName
   *          The data schema name to be returned.
   * @return The data schema, or <code>null</code> if no such schema.
   */
  public static DataSchema get(String schemaName)
  {
    return registry.get(schemaName);
  }

  /**
   * Returns the set of data schema names held in the registry.
   * 
   * @return The possibly empty set of data schema names.
   */
  public static Set<String> getNames()
  {
    return registry.keySet();
  }

  /**
   * Clear the registry
   */
  public static void clearRegistry()
  {
    registry.clear();
  }
}
