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
package org.apache.pirk.schema.response;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.apache.pirk.schema.query.QuerySchema;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON helper class for query results
 * <p>
 * 
 */
public class QueryResponseJSON implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(QueryResponseJSON.class);

  private JSONObject jsonObj = null;

  private DataSchema dSchema = null;

  private QueryInfo queryInfo = null;

  public static final String EVENT_TYPE = "event_type"; // notification type the matched the record
  public static final Text EVENT_TYPE_TEXT = new Text(EVENT_TYPE);

  public static final String QUERY_ID = "query_id"; // query ID that generated the notification
  public static final Text QUERY_ID_TEXT = new Text(QUERY_ID);

  public static final String QUERY_NAME = "query_name"; // name of the query that generated the notification
  public static final Text QUERY_NAME_TEXT = new Text(QUERY_NAME);

  public static final String SELECTOR = "match"; // tag for selector that generated the hit
  public static final Text SELECTOR_TEXT = new Text(SELECTOR);

  /**
   * Constructor with data schema checking
   */
  public QueryResponseJSON(QueryInfo queryInfoIn)
  {
    queryInfo = queryInfoIn;

    if (queryInfo == null)
    {
      logger.info("queryInfo is null");
    }

    QuerySchema qSchema = LoadQuerySchemas.getSchema(queryInfo.getQueryType());
    dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());

    jsonObj = new JSONObject();
    setGeneralQueryResponseFields(queryInfo);
  }

  /**
   * Constructor with no data schema checking
   */
  public QueryResponseJSON()
  {
    jsonObj = new JSONObject();
  }

  /**
   * Constructor with no data schema checking
   */
  public QueryResponseJSON(String jsonString)
  {
    jsonObj = (JSONObject) JSONValue.parse(jsonString);
  }

  public JSONObject getJSONObject()
  {
    return jsonObj;
  }

  public String getJSONString()
  {
    return jsonObj.toString();
  }

  public Object getValue(String key)
  {
    return jsonObj.get(key);
  }

  public QueryInfo getQueryInfo()
  {
    return queryInfo;
  }

  // Create empty JSON object based on the DataSchema
  @SuppressWarnings("unchecked")
  private void initialize()
  {
    Set<String> schemaStringRep = dSchema.getNonArrayElements();
    for (String key : schemaStringRep)
    {
      jsonObj.put(key, "");
    }
    Set<String> schemaListRep = dSchema.getArrayElements();
    for (String key : schemaListRep)
    {
      jsonObj.put(key, new ArrayList<>());
    }
  }

  /**
   * Add a <key,value> pair to the response object; checks the data schema if this QueryResponseJSON object was instantiated with schema checking (with a
   * QueryInfo object)
   */
  @SuppressWarnings("unchecked")
  public void setMapping(String key, Object val)
  {
    if (dSchema == null)
    {
      jsonObj.put(key, val);
    }
    else
    {
      if (dSchema.getArrayElements().contains(key))
      {
        if (!(val instanceof ArrayList))
        {
          ArrayList<Object> list;
          if (!jsonObj.containsKey(key))
          {
            list = new ArrayList<>();
            jsonObj.put(key, list);
          }
          list = (ArrayList<Object>) jsonObj.get(key);

          if (!list.contains(val))
          {
            list.add(val);
          }
          jsonObj.put(key, list);
        }
        else
        {
          jsonObj.put(key, val);
        }
      }
      else if (dSchema.getNonArrayElements().contains(key) || key.equals(SELECTOR))
      {
        jsonObj.put(key, val);
      }
      else
      {
        logger.info("WARN: Schema does not contain key = " + key);
      }
    }
  }

  // Method to set the selector field explicitly
  @SuppressWarnings("unchecked")
  public void setSelector(Object val)
  {
    jsonObj.put(SELECTOR, val);
  }

  public void setAllFields(HashMap<String,String> dataMap)
  {
    for (String key : dataMap.keySet())
    {
      setMapping(key, dataMap.get(key));
    }
  }

  /**
   * Method to set the common query response fields
   */
  @SuppressWarnings("unchecked")
  public void setGeneralQueryResponseFields(QueryInfo queryInfo)
  {
    jsonObj.put(EVENT_TYPE, queryInfo.getQueryType());
    jsonObj.put(QUERY_ID, queryInfo.getQueryNum());
    jsonObj.put(QUERY_NAME, queryInfo.getQueryName());
  }

  @Override
  public String toString()
  {
    return jsonObj.toString();
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((jsonObj == null) ? 0 : jsonObj.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QueryResponseJSON other = (QueryResponseJSON) obj;
    if (jsonObj == null)
    {
      if (other.jsonObj != null)
        return false;
    }
    else
    {
      Set<String> thisKeySet = jsonObj.keySet();
      Set<String> otherKeySet = other.jsonObj.keySet();
      if (!thisKeySet.equals(otherKeySet))
      {
        return false;
      }
      for (String key : thisKeySet)
      {
        if (!(jsonObj.get(key)).equals(other.jsonObj.get(key)))
        {
          return false;
        }
      }
    }
    return true;
  }
}
