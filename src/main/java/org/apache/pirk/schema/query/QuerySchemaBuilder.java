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
package org.apache.pirk.schema.query;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.data.partitioner.DataPartitioner;
import org.apache.pirk.schema.query.filter.DataFilter;
import org.apache.pirk.schema.query.filter.FilterFactory;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The query schema builder is used to compose a new {@link QuerySchema} .
 * <p>
 * Each schema is required to have a name, a primary selector name, and a reference to a data schema that has already been registered, and a set of element
 * names in that data schema that will be the subject of the query.
 * <p>
 * Optionally a query schema can define a filter to apply to data elements before performing the encrypted query.
 * <p>
 * A builder can be used to define each of these characteristics, before calling the {@code build()} method to create the query schema.
 */
public class QuerySchemaBuilder
{
  private static final Logger logger = LoggerFactory.getLogger(QuerySchemaBuilder.class);

  private static final String NO_FILTER = "noFilter";

  private String name;
  private String dataSchemaName;
  private String selectorName;
  private Set<String> queryElementNames = Collections.emptySet();
  private String filterTypeName = NO_FILTER;
  private Set<String> filteredElementNames = Collections.emptySet();
  private Map<String,String> additionalFields = Collections.emptyMap();

  /**
   * Created a new query schema builder.
   */
  public QuerySchemaBuilder()
  {
    // Default constructor
  }

  /**
   * Builds a new query schema using the information set on the builder.
   * 
   * @return The query schema.
   * @throws IOException
   *           If an error occurred creating the defined query element filter.
   * @throws PIRException
   *           If the query schema definition is invalid. The exception message will give the details of the problem.
   */
  public QuerySchema build() throws IOException, PIRException
  {
    verifySchemaProperties();
    verifyDataSchemaProperties();
    verifyQueryElements();

    DataFilter filter = instantiateFilter(filterTypeName, filteredElementNames);
    QuerySchema schema = new QuerySchema(name, dataSchemaName, selectorName, filterTypeName, filter, computeDataElementSize());
    schema.getElementNames().addAll(queryElementNames);
    schema.getFilteredElementNames().addAll(filteredElementNames);
    schema.getAdditionalFields().putAll(additionalFields);

    return schema;
  }

  /**
   * Returns the name of the query schema being created.
   * 
   * @return The query schema name.
   */
  public String getName()
  {
    return name;
  }

  /**
   * Sets the name of the query schema being created.
   * 
   * @param name
   *          The schema name.
   * @return this builder.
   */
  public QuerySchemaBuilder setName(String name)
  {
    this.name = name;
    return this;
  }

  /**
   * Returns the data schema associated with this query schema.
   * 
   * @return The name of the data schema to set on the query schema.
   */
  public String getDataSchemaName()
  {
    return dataSchemaName;
  }

  /**
   * Sets the data schema associated with this query schema.
   * 
   * @param dataSchemaName
   *          The name of the data schema to set on the query schema.
   * @return this builder.
   */
  public QuerySchemaBuilder setDataSchemaName(String dataSchemaName)
  {
    this.dataSchemaName = dataSchemaName;
    return this;
  }

  /**
   * Returns the names of the data schema that are the subject of this query schema.
   * 
   * @return A possibly empty set of data element names to return as part of the query.
   */
  public Set<String> getQueryElementNames()
  {
    return queryElementNames;
  }

  /**
   * Sets the names of the data schema that are the subject of this query schema.
   * 
   * @param elementNames
   *          The set of data element names to return as part of the query.
   * @return This builder.
   */
  public QuerySchemaBuilder setQueryElementNames(Set<String> elementNames)
  {
    this.queryElementNames = elementNames;
    return this;
  }

  /**
   * Returns the element used as the primary selector for the query.
   * 
   * @return the name of the element in the data schema that is to be used as the primary selector for the query.
   */
  public String getSelectorName()
  {
    return selectorName;
  }

  /**
   * Sets the element used as the primary selector for the query
   * 
   * @param selectorName
   *          The name of the element in the data schema that is to be used as the primary selector for the query.
   * @return This builder.
   */
  public QuerySchemaBuilder setSelectorName(String selectorName)
  {
    this.selectorName = selectorName;
    return this;
  }

  /**
   * Returns the name of a filter to use with this query schema.
   * 
   * @return The fully qualified class name of a type that implements {@link DataFilter}.
   */
  public String getFilterTypeName()
  {
    return filterTypeName;
  }

  /**
   * Sets the name of a filter to use with this query schema.
   * 
   * @param filterTypeName
   *          The fully qualified class name of a type that implements {@link DataFilter}.
   * @return This builder.
   */
  public QuerySchemaBuilder setFilterTypeName(String filterTypeName)
  {
    this.filterTypeName = filterTypeName;
    return this;
  }

  /**
   * Returns the set of names to be filtered when using this query schema.
   * 
   * @return A possibly empty set of data schema element names to be filtered.
   */
  public Set<String> getFilteredElementNames()
  {
    return filteredElementNames;
  }

  /**
   * Sets the names to be filtered when using this query schema.
   * 
   * @param filteredElementNames
   *          The set of data schema element names to be filtered.
   * @return This builder.
   */
  public QuerySchemaBuilder setFilteredElementNames(Set<String> filteredElementNames)
  {
    this.filteredElementNames = filteredElementNames;
    return this;
  }

  /**
   * Returns the key:value pairs to be set on the query schema.
   * 
   * @return A map of arbitrary key value pairs.
   */
  public Map<String,String> getAdditionalFields()
  {
    return additionalFields;
  }

  /**
   * Sets the additional key:value pairs to be set on the query schema.
   * 
   * @param additionalFields
   *          The map of key:value pairs to be defined on the query schema.
   * @return This builder.
   */
  public QuerySchemaBuilder setAdditionalFields(Map<String,String> additionalFields)
  {
    this.additionalFields = additionalFields;
    return this;
  }

  private DataSchema getDataSchema() throws PIRException
  {
    if (dataSchemaName == null)
    {
      throw new PIRException("Required data schema name is not set");
    }

    DataSchema dataSchema = DataSchemaRegistry.get(dataSchemaName);
    if (dataSchema == null)
    {
      throw new PIRException("Loaded DataSchema does not exist for dataSchemaName = " + dataSchemaName);
    }
    return dataSchema;
  }

  private int computeDataElementSize() throws PIRException
  {
    DataSchema dataSchema = getDataSchema();
    int dataElementSize = 0;
    for (String elementName : queryElementNames)
    {
      // Compute the number of bits for this element.
      DataPartitioner partitioner = dataSchema.getPartitionerForElement(elementName);
      int bits = partitioner.getBits(dataSchema.getElementType(elementName));

      // Multiply by the number of array elements allowed, if applicable.
      if (dataSchema.isArrayElement(elementName))
      {
        bits *= Integer.parseInt(SystemConfiguration.getProperty("pir.numReturnArrayElements"));
      }
      dataElementSize += bits;

      logger.info("name = " + elementName + " bits = " + bits + " dataElementSize = " + dataElementSize);
    }

    return dataElementSize;
  }

  /**
   * Instantiate the specified filter.
   *
   * Exceptions derive from call to the {@code getFilter} method of {@link FilterFactory}
   * 
   * @param filterTypeName
   *          The name of the filter class we are instantiating
   * @param filteredElementNames
   *          The set of names of elements of the data schema up which the filter will act.
   * @return An instantiation of the filter, set up to filter upon the specified names.
   * @throws IOException
   *           - failed to read input
   * @throws PIRException
   *           - File could not be instantiated
   */
  private DataFilter instantiateFilter(String filterTypeName, Set<String> filteredElementNames) throws IOException, PIRException
  {
    return filterTypeName.equals(NO_FILTER) ? null : FilterFactory.getFilter(filterTypeName, filteredElementNames);
  }

  private void verifyDataSchemaProperties() throws PIRException
  {
    // We must have a matching data schema for this query.
    DataSchema dataSchema = getDataSchema();

    // Ensure the selectorName matches an element in the data schema.
    if (!dataSchema.containsElement(selectorName))
    {
      throw new PIRException("dataSchema = " + dataSchemaName + " does not contain selector = " + selectorName);
    }
  }

  private void verifyQueryElements() throws PIRException
  {
    DataSchema dataSchema = getDataSchema();

    for (String elementName : queryElementNames)
    {
      if (!dataSchema.containsElement(elementName))
      {
        throw new PIRException("dataSchema = " + dataSchemaName + " does not contain requested element name = " + elementName);
      }
      logger.info("name = " + elementName + " partitionerName = " + dataSchema.getPartitionerTypeName(elementName));
    }
  }

  private void verifySchemaProperties() throws PIRException
  {
    if (name == null)
    {
      throw new PIRException("Required property query schema name is not set");
    }
    // Other required properties are checked by other helpers.
  }
}
