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

import java.io.Serializable;
import java.util.HashSet;
import java.util.TreeSet;

import org.apache.pirk.schema.query.filter.FilterFactory;

/**
 * Class to hold a query schema
 * <p>
 * TODO:
 * <p>
 * -Could easily add the ability for multiple filters (filter list capability) instead of just one
 */
public class QuerySchema implements Serializable
{
  private static final long serialVersionUID = 1L;

  static final String NO_FILTER = "noFilter";

  private String schemaName = null;

  private String dataSchemaName = null; // name of the DataSchema for this query schema

  private TreeSet<String> elementNames = null; // names of elements in the data schema to
  // include in the response, order matters for packing/unpacking

  private String filter = null; // name of filter class to use in data filtering

  private Object filterObj = null; // instance of the filter

  private HashSet<String> filterElementNames = null; // set of element names to apply filtering in pre-processing

  private String selectorName = null; // name of element in the dataSchema to be used as the selector

  private int dataElementSize = 0; // total number of bits to be returned for each data element hit

  public QuerySchema(String schemaNameInput, String dataSchemaNameInput, TreeSet<String> elementNamesInput, String selectorNameInput, int dataElementSizeInput,
      HashSet<String> filterElementNamesInput, String filterIn) throws Exception
  {
    schemaName = schemaNameInput;
    dataSchemaName = dataSchemaNameInput;
    elementNames = elementNamesInput;
    selectorName = selectorNameInput;
    dataElementSize = dataElementSizeInput;
    filterElementNames = filterElementNamesInput;
    filter = filterIn;

    instantiateFilter();
  }

  public String getSchemaName()
  {
    return schemaName;
  }

  public String getDataSchemaName()
  {
    return dataSchemaName;
  }

  public TreeSet<String> getElementNames()
  {
    return elementNames;
  }

  public String getSelectorName()
  {
    return selectorName;
  }

  public int getDataElementSize()
  {
    return dataElementSize;
  }

  /**
   * Method to get the name of the filter class for this query
   */
  public String getFilter()
  {
    return filter;
  }

  public HashSet<String> getFilterElementNames()
  {
    return filterElementNames;
  }

  /**
   * Method to return the instance of the specified filter for this query
   * <p>
   * Will return null if no filter has been specified for the query
   */
  public Object getFilterInstance() throws Exception
  {
    instantiateFilter();

    return filterObj;
  }

  private void instantiateFilter() throws Exception
  {
    if (!filter.equals(NO_FILTER))
    {
      if (filterObj == null)
      {
        filterObj = FilterFactory.getFilter(filter, this);
      }
    }
  }
}
