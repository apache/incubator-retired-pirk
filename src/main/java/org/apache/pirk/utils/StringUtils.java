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
package org.apache.pirk.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pirk.schema.data.DataSchema;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pirk-specific string utilities
 * 
 */
public class StringUtils
{
  private static final Logger logger = LoggerFactory.getLogger(StringUtils.class);

  /**
   * Method to convert a MapWritable into a JSON string
   * 
   */
  @SuppressWarnings("unchecked")
  public static String mapWritableToString(MapWritable map)
  {
    // Convert to JSON and then write to a String - ensures JSON read-in compatibility
    JSONObject jsonObj = new JSONObject();
    for (Writable key : map.keySet())
    {
      jsonObj.put(key.toString(), map.get(key).toString());
    }

    return jsonObj.toJSONString();
  }

  /**
   * Method to take an input json string and output a MapWritable with arrays as JSON formatted String objects
   */
  public static MapWritable jsonStringToMapWritable(String jsonString)
  {
    MapWritable value = new MapWritable();
    JSONParser jsonParser = new JSONParser();

    try
    {
      JSONObject jsonObj = (JSONObject) jsonParser.parse(jsonString);
      for (Object key : jsonObj.keySet())
      {
        Text mapKey = new Text(key.toString());
        Text mapValue = new Text();
        if (jsonObj.get(key) != null)
        {
          mapValue.set(jsonObj.get(key).toString());
        }
        value.put(mapKey, mapValue);
      }
    } catch (ParseException e)
    {
      logger.warn("Could not json-decode string: " + jsonString, e);
    } catch (NumberFormatException e)
    {
      logger.warn("Could not parse field into number: " + jsonString, e);
    }

    return value;
  }

  /**
   * Method to take an input json string and output a MapWritable with arrays as WritableArrayWritable objects
   */
  public static MapWritable jsonStringToMapWritableWithWritableArrayWritable(String jsonString, DataSchema dataSchema)
  {
    MapWritable value = new MapWritable();
    JSONParser jsonParser = new JSONParser();

    try
    {
      JSONObject jsonObj = (JSONObject) jsonParser.parse(jsonString);
      for (Object key : jsonObj.keySet())
      {
        Text mapKey = new Text(key.toString());
        if (jsonObj.get(key) != null)
        {
          logger.debug("key = " + key.toString());
          if (dataSchema.hasListRep((String) key))
          {
            WritableArrayWritable mapValue = StringUtils.jsonArrayStringToWritableArrayWritable(jsonObj.get(key).toString());
            value.put(mapKey, mapValue);
          }
          else
          {
            Text mapValue = new Text(jsonObj.get(key).toString());
            value.put(mapKey, mapValue);
          }
        }
      }
    } catch (ParseException e)
    {
      logger.warn("Could not json-decode string: " + jsonString, e);
    } catch (NumberFormatException e)
    {
      logger.warn("Could not parse field into number: " + jsonString, e);
    }

    return value;
  }

  /**
   * Method to take an input json string and output a MapWritable with arrays as WritableArrayWritable objects
   */
  public static MapWritable jsonStringToMapWritableWithArrayWritable(String jsonString, DataSchema dataSchema)
  {
    MapWritable value = new MapWritable();
    JSONParser jsonParser = new JSONParser();

    try
    {
      JSONObject jsonObj = (JSONObject) jsonParser.parse(jsonString);
      for (Object key : jsonObj.keySet())
      {
        Text mapKey = new Text(key.toString());
        if (jsonObj.get(key) != null)
        {
          logger.debug("key = " + key.toString());
          if (dataSchema.hasListRep((String) key))
          {
            ArrayWritable mapValue = StringUtils.jsonArrayStringtoArrayWritable(jsonObj.get(key).toString());
            value.put(mapKey, mapValue);
          }
          else
          {
            Text mapValue = new Text(jsonObj.get(key).toString());
            value.put(mapKey, mapValue);
          }
        }
      }
    } catch (ParseException e)
    {
      logger.warn("Could not json-decode string: " + jsonString, e);
    } catch (NumberFormatException e)
    {
      logger.warn("Could not parse field into number: " + jsonString, e);
    }

    return value;
  }

  /**
   * Method to take an input json string and output a Map<String, Object> with arrays as ArrayList<String> objects and single values as String objects
   */
  public static Map<String,Object> jsonStringToMap(String jsonString, DataSchema dataSchema)
  {
    Map<String,Object> value = new HashMap<>();
    JSONParser jsonParser = new JSONParser();

    try
    {
      JSONObject jsonObj = (JSONObject) jsonParser.parse(jsonString);
      for (Object key : jsonObj.keySet())
      {
        String mapKey = key.toString();
        if (jsonObj.get(key) != null)
        {
          if (dataSchema.hasListRep((String) key))
          {
            ArrayList<String> mapValue = StringUtils.jsonArrayStringToArrayList(jsonObj.get(key).toString());
            value.put(mapKey, mapValue);
          }
          else
          {
            value.put(mapKey, jsonObj.get(key).toString());
          }
        }
      }
    } catch (ParseException e)
    {
      logger.warn("Could not json-decode string: " + jsonString, e);
    } catch (NumberFormatException e)
    {
      logger.warn("Could not parse field into number: " + jsonString, e);
    }

    return value;
  }

  /**
   * Method to take an input json array format string and output a WritableArrayWritable
   */
  public static WritableArrayWritable jsonArrayStringToWritableArrayWritable(String jsonString)
  {
    String modString = jsonString.replaceFirst("\\[", "");
    modString = modString.replaceFirst("\\]", "");
    modString = modString.replaceAll("\"", "");
    String[] elements = modString.split("\\s*,\\s*");
    logger.debug("elements = ");
    for (String element : elements)
    {
      logger.debug("element: " + element);
    }

    return new WritableArrayWritable(elements);
  }

  /**
   * Method to take an input json array format string and output an ArrayWritable
   */
  public static ArrayWritable jsonArrayStringtoArrayWritable(String jsonString)
  {
    String modString = jsonString.replaceFirst("\\[", "");
    modString = modString.replaceFirst("\\]", "");
    modString = modString.replaceAll("\"", "");
    String[] elements = modString.split("\\s*,\\s*");
    logger.debug("elements = ");
    for (String element : elements)
    {
      logger.debug("element: " + element);
    }

    return new ArrayWritable(elements);
  }

  /**
   * Method to take an input json array format string and output an ArrayList
   */
  public static ArrayList<String> jsonArrayStringToArrayList(String jsonString)
  {
    String modString = jsonString.replaceFirst("\\[", "");
    modString = modString.replaceFirst("\\]", "");
    modString = modString.replaceAll("\"", "");
    String[] elements = modString.split("\\s*,\\s*");

    return new ArrayList<>(Arrays.asList(elements));
  }

  /**
   * Method to take an input json array format string and output a String array
   */
  public static String[] jsonArrayStringToList(String jsonString)
  {
    String modString = jsonString.replaceFirst("\\[", "");
    modString = modString.replaceFirst("\\]", "");
    modString = modString.replaceAll("\"", "");
    return modString.split("\\s*,\\s*");
  }
}
