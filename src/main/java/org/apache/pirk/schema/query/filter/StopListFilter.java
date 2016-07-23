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
package org.apache.pirk.schema.query.filter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.utils.StopListUtils;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter class to filter data elements based upon a stoplist applied to specified field elements
 */
public class StopListFilter implements DataFilter
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(StopListFilter.class);

  private HashSet<String> filterSet = null;
  private HashSet<String> stopList = null;

  public StopListFilter(HashSet<String> filterSetIn, HashSet<String> stopListIn)
  {
    filterSet = filterSetIn;
    stopList = stopListIn;
  }

  @Override
  public boolean filterDataElement(MapWritable dataElement, DataSchema dSchema)
  {
    boolean passFilter = true;

    // If the data element contains a value on the stoplist (corresponding to a key in the filterSet), do not use
    for (String filterName : filterSet)
    {
      if (dSchema.hasListRep(filterName))
      {
        List<String> elementArray = null;
        if (dataElement.get(dSchema.getTextElement(filterName)) instanceof WritableArrayWritable)
        {
          elementArray = Arrays.asList(((WritableArrayWritable) dataElement.get(dSchema.getTextElement(filterName))).toStrings());
        }
        else if (dataElement.get(dSchema.getTextElement(filterName)) instanceof ArrayWritable)
        {
          elementArray = Arrays.asList(((ArrayWritable) dataElement.get(dSchema.getTextElement(filterName))).toStrings());
        }

        for (String element : elementArray)
        {
          passFilter = StopListUtils.checkElement(element, stopList);
          if (!passFilter)
          {
            break;
          }
        }
      }
      else
      {
        String element = dataElement.get(dSchema.getTextElement(filterName)).toString();
        passFilter = StopListUtils.checkElement(element, stopList);
      }
      if (!passFilter)
      {
        break;
      }
    }
    return passFilter;
  }
}
