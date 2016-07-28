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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.pirk.inputformat.hadoop.TextArrayWritable;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.partitioner.IPDataPartitioner;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for URI query parsing
 * <p>
 * Query of the form @{code ?q=<queryField>:<queryTerm>}
 * <p>
 * TODO: Use Lucene's query parsing?... Make this lots lots better...
 *
 */
public class QueryParserUtils
{
  private static final Logger logger = LoggerFactory.getLogger(QueryParserUtils.class);

  /**
   * Given a URI query string, checks to see if the given document satisfies the query
   * <p>
   * ...Very hacky...
   * <p>
   * NOTE: Assumes that MapWritable keys are Text objects and values are Text or TextArrayWritable objects
   * <p>
   * NOTE: Support for list fields (values) is provided for checkRecord with Map<String, Object> and checkRecord with MapWritable containing
   * WritableArrayWritable types for array values (vs. json string list representation)
   * <p>
   * NOTE: @ symbol represents flag ignore case sensitivity used after field (e.g. user_agent@:*searchparam*)
   * <p>
   * NOTE: Assumes that all AND booleans come before any OR booleans - ADD @ case sensitivity support for range queries
   * 
   */
  public static boolean checkRecord(String uriQuery, MapWritable doc, DataSchema dataSchema)
  {
    boolean satisfiesQuery = true;

    logger.debug("uriQuery = " + uriQuery);
    uriQuery = uriQuery.substring(3); // strip the beginning query tag '?q='
    logger.debug("uriQuery = " + uriQuery);

    if (uriQuery.equals("*"))
    {
      return true;
    }

    String[] queryTokens = uriQuery.split("\\+(?=AND)|\\+(?=OR)|\\+(?=[a-z])"); // booleans of the form +AND+, +OR+, don't split on +T0+
    int index = 0;
    String item;
    while (index < queryTokens.length)
    {
      boolean ignoreCase = false;

      item = queryTokens[index];
      logger.debug("item = " + item);

      String[] itemTokens = item.split(":", 2); // There are two components <field>:<query>
      logger.debug("itemTokens[0] = " + itemTokens[0] + " itemTokens[1] = " + itemTokens[1]);

      // check for ignore case flag
      if (itemTokens[0].endsWith("@"))
      {
        ignoreCase = true;
        logger.debug("ignore case = true");
        itemTokens[0] = itemTokens[0].replaceAll("@", ""); // strip flag
        logger.debug("itemTokens[0]:" + itemTokens[0]);
      }

      Object value = doc.get(new Text(itemTokens[0]));
      if (value != null) // if the field is not present, a null Writable is returned
      {

        if (itemTokens[1].startsWith("[")) // Inclusive range query
        {
          if (value instanceof Text)
          {
            if (!checkRangeQuery(true, itemTokens[0], itemTokens[1], value.toString(), dataSchema))
            {
              logger.debug("checkRangeQuery returned false");
              satisfiesQuery = false;
            }
          }
          else if (value instanceof TextArrayWritable)
          {
            String[] elements = ((TextArrayWritable) value).toStrings();
            boolean oneSatisfied = false;
            for (String element : elements)
            {
              if (checkRangeQuery(true, itemTokens[0], itemTokens[1], element, dataSchema))
              {
                logger.debug("checkRangeQuery returned true");
                oneSatisfied = true;
                break;
              }
            }
            satisfiesQuery = oneSatisfied;
          }
        }
        else if (itemTokens[1].startsWith("{")) // Exclusive range query
        {
          if (value instanceof Text)
          {
            if (!checkRangeQuery(false, itemTokens[0], itemTokens[1], value.toString(), dataSchema))
            {
              logger.debug("checkRangeQuery returned false");
              satisfiesQuery = false;
            }
          }
          else if (value instanceof TextArrayWritable)
          {
            String[] elements = ((TextArrayWritable) value).toStrings();
            boolean oneSatisfied = false;
            for (String element : elements)
            {
              if (checkRangeQuery(false, itemTokens[0], itemTokens[1], element, dataSchema))
              {
                logger.debug("checkRangeQuery returned true");
                oneSatisfied = true;
                break;
              }
            }
            satisfiesQuery = oneSatisfied;
          }
        }
        else
        // Not a range query
        {
          if (value instanceof Text)
          {
            String valueString = value.toString();
            if (ignoreCase)
            { // Case insensitivity
              logger.debug("not a range query; itemstoken1:" + itemTokens[1]);
              itemTokens[1] = itemTokens[1].toLowerCase();
              valueString = valueString.toLowerCase();
              logger.debug("valuestring after:" + valueString);
            }

            if (itemTokens[1].contains("*") || itemTokens[1].contains("?")) // Wildcard match
            {
              logger.debug("itemTokens[1] = " + itemTokens[1] + " contains wildcard");
              if (!Pattern.matches(wildcardToRegex(itemTokens[1]), valueString))
              {
                logger.debug("stringValue = " + valueString + " did not satisfy itemTokens[1] = " + itemTokens[1]);
                satisfiesQuery = false;
              }
              logger.debug("stringValue = " + valueString + " did satisfy itemTokens[1] = " + itemTokens[1]);
            }
            else if (!(valueString).equals(itemTokens[1])) // Single value match
            {
              logger.debug("We do not have a single value match: stringValue " + valueString + " != itemTokens[1] = " + itemTokens[1]);
              satisfiesQuery = false;
            }
          }
          else if (value instanceof TextArrayWritable)
          {
            String[] elements = ((TextArrayWritable) value).toStrings();
            logger.debug("elements.size() = " + elements.length);

            boolean oneSatisfied = false;
            for (String element : elements)
            {
              if (ignoreCase)
              { // Case insensitivity
                itemTokens[1] = itemTokens[1].toLowerCase();
                logger.debug("waw: itemtoken1 after:" + itemTokens[1]);
                element = element.toLowerCase();
                logger.debug("element after:" + element);
              }

              logger.debug("element: " + element);
              if (itemTokens[1].contains("*") || itemTokens[1].contains("?")) // Wildcard match
              {
                logger.debug("itemTokens[1] = " + itemTokens[1] + " contains wildcard");
                if (Pattern.matches(wildcardToRegex(itemTokens[1]), element))
                {
                  logger.debug("stringValue = " + element + " satisfied itemTokens[1] = " + itemTokens[1]);
                  oneSatisfied = true;
                  break;
                }
              }
              else if (element.equals(itemTokens[1])) // Single value match
              {
                logger.debug("We have a single value match: stringValue " + element + " = itemTokens[1] = " + itemTokens[1]);
                oneSatisfied = true;
                break;
              }
            }
            satisfiesQuery = oneSatisfied;
          }
        }
      }
      else
      {
        satisfiesQuery = false; // add fix - account for case if query field does not exist
      }

      ++index; // Try to pick up the boolean operators
      if (index < (queryTokens.length - 1))
      {
        if (queryTokens[index].equals("AND")) // Do nothing and keep going
        {
          if (!satisfiesQuery)
          {
            break;
          }
          ++index;
          item = queryTokens[index];
        }
        else if (queryTokens[index].equals("OR")) // Assume all OR's occur after all AND's
        {
          if (satisfiesQuery) // if we passed the query and it is not the first term
          {
            break;
          }
          else
          {
            ++index;
            item = queryTokens[index];
            satisfiesQuery = true; // reset so that we pick up matches for the next term
          }
        }
        else if (!satisfiesQuery)
        {
          logger.debug("Does not satisfy the query and no boolean ops next...");
          break;
        }
      }
    }

    return satisfiesQuery;
  }

  @SuppressWarnings("unchecked")
  public static boolean checkRecord(String uriQuery, Map<String,Object> doc, DataSchema dataSchema)
  {
    boolean satisfiesQuery = true;

    logger.debug("uriQuery = " + uriQuery);
    uriQuery = uriQuery.substring(3); // strip the beginning query tag '?q='
    logger.debug("uriQuery = " + uriQuery);

    if (uriQuery.equals("*"))
    {
      return true;
    }

    String[] queryTokens = uriQuery.split("\\+(?=AND)|\\+(?=OR)|\\+(?=[a-z])"); // booleans of the form +AND+, +OR+, don't split on +T0+
    int index = 0;
    String item;
    while (index < queryTokens.length)
    {
      item = queryTokens[index];
      logger.debug("item = " + item);

      String[] itemTokens = item.split(":", 2); // There are two components <field>:<query>
      logger.debug("itemTokens[0] = " + itemTokens[0] + " itemTokens[1] = " + itemTokens[1]);

      boolean ignoreCase = false;

      // check for ignore case flag
      if (itemTokens[0].endsWith("@"))
      {
        ignoreCase = true;
        itemTokens[0] = itemTokens[0].replaceAll("@", ""); // strip case flag
        logger.debug("ignore case: itemTokens[0] = " + itemTokens[0] + " itemTokens[1] = " + itemTokens[1]);
      }

      Object value = doc.get(itemTokens[0]); // handle array answers
      if (value != null) // if the field is not present, a null Writable is returned
      {
        if (itemTokens[1].startsWith("[")) // Inclusive range query
        {
          if (value instanceof String)
          {
            if (ignoreCase)
            { // case insensitivity
              itemTokens[1] = itemTokens[1].toLowerCase();
              value = value.toString().toLowerCase();
            }

            if (!checkRangeQuery(true, itemTokens[0], itemTokens[1], (String) value, dataSchema))
            {
              logger.debug("checkRangeQuery returned false");
              satisfiesQuery = false;
            }
          }
          else if (value instanceof ArrayList)
          {
            boolean oneSatisfied = false;
            for (String element : (ArrayList<String>) value)
            {
              if (checkRangeQuery(true, itemTokens[0], itemTokens[1], element, dataSchema))
              {
                logger.debug("checkRangeQuery returned true");
                oneSatisfied = true;
                break;
              }
            }
            satisfiesQuery = oneSatisfied;
          }
        }
        else if (itemTokens[1].startsWith("{")) // Exclusive range query
        {
          if (value instanceof String)
          {
            if (!checkRangeQuery(false, itemTokens[0], itemTokens[1], (String) value, dataSchema))
            {
              logger.debug("checkRangeQuery returned false");
              satisfiesQuery = false;
            }
          }
          else if (value instanceof ArrayList)
          {

            boolean oneSatisfied = false;
            for (String element : (ArrayList<String>) value)
            {
              if (ignoreCase)
              {
                itemTokens[1] = itemTokens[1].toLowerCase();
                element = element.toLowerCase();
              }
              if (checkRangeQuery(false, itemTokens[0], itemTokens[1], element, dataSchema))
              {
                logger.debug("checkRangeQuery returned true");
                oneSatisfied = true;
                break;
              }
            }
            satisfiesQuery = oneSatisfied;
          }
        }
        else
        // Not a range query
        {
          if (value instanceof String)
          {
            if (ignoreCase)
            {
              itemTokens[1] = itemTokens[1].toLowerCase();
              value = value.toString().toLowerCase();
            }

            if (itemTokens[1].contains("*") || itemTokens[1].contains("?")) // Wildcard match
            {
              logger.debug("itemTokens[1] = " + itemTokens[1] + " contains wildcard");
              if (!Pattern.matches(wildcardToRegex(itemTokens[1]), (String) value))
              {
                logger.debug("stringValue = " + value + " did not satisfy itemTokens[1] = " + itemTokens[1]);
                satisfiesQuery = false;
              }
              logger.debug("stringValue = " + value + " did satisfy itemTokens[1] = " + itemTokens[1]);
            }
            else if (!(value).equals(itemTokens[1])) // Single value match
            {
              logger.debug("We do not have a single value match: stringValue " + (String) value + " != itemTokens[1] = " + itemTokens[1]);
              satisfiesQuery = false;
            }
          }
          else if (value instanceof ArrayList)
          {
            boolean oneSatisfied = false;
            for (String element : (ArrayList<String>) value)
            {
              logger.debug("element = " + element);
              if (ignoreCase)
              { // case insensitivity
                itemTokens[1] = itemTokens[1].toLowerCase();
                element = element.toLowerCase();
              }

              if (itemTokens[1].contains("*") || itemTokens[1].contains("?")) // Wildcard match
              {
                logger.debug("itemTokens[1] = " + itemTokens[1] + " contains wildcard");
                if (Pattern.matches(wildcardToRegex(itemTokens[1]), element))
                {
                  logger.debug("stringValue = " + element + " satisfied itemTokens[1] = " + itemTokens[1]);
                  oneSatisfied = true;
                  break;
                }
              }
              else if (element.equals(itemTokens[1])) // Single value match
              {
                logger.debug("We have a single value match: stringValue " + element + " = itemTokens[1] = " + itemTokens[1]);
                oneSatisfied = true;
                break;
              }
            }
            satisfiesQuery = oneSatisfied;
          }
        }
      }
      else
      { // added fix: If the value is null (the requested field did not appear in the data)
        satisfiesQuery = false;
      }
      ++index; // Try to pick up the boolean operators
      if (index < (queryTokens.length - 1))
      {
        if (queryTokens[index].equals("AND")) // Do nothing and keep going
        {
          if (!satisfiesQuery)
          {
            break;
          }
          ++index;
          item = queryTokens[index];
        }
        else if (queryTokens[index].equals("OR")) // Assume all OR's occur after all AND's
        {
          if (satisfiesQuery && (index != 1)) // if we passed the query and it is not the first term
          {
            break;
          }
          else
          {
            ++index;
            item = queryTokens[index];
            satisfiesQuery = true; // reset so that we pick up matches for the next term
          }
        }
        else if (!satisfiesQuery)
        {
          logger.debug("Does not satisfy the query and no boolean ops next...");
          break;
        }
      }
    }

    return satisfiesQuery;
  }

  @SuppressWarnings("unchecked")
  public static boolean checkRecordWritableArrayWritable(String uriQuery, MapWritable doc, DataSchema dataSchema)
  {
    boolean satisfiesQuery = true;

    logger.debug("uriQuery = " + uriQuery);
    uriQuery = uriQuery.substring(3); // strip the beginning query tag '?q='
    logger.debug("uriQuery = " + uriQuery);

    if (uriQuery.equals("*"))
    {
      return true;
    }

    String[] queryTokens = uriQuery.split("\\+(?=AND)|\\+(?=OR)|\\+(?=[a-z])"); // booleans of the form +AND+, +OR+, don't split on +T0+
    int index = 0;
    String item;
    while (index < queryTokens.length)
    {
      boolean ignoreCase = false;

      item = queryTokens[index];
      logger.debug("item = " + item);

      String[] itemTokens = item.split(":", 2); // There are two components <field>:<query>
      logger.debug("itemTokens[0] = " + itemTokens[0] + " itemTokens[1] = " + itemTokens[1]);

      // check for ignore case flag
      if (itemTokens[0].endsWith("@"))
      {
        ignoreCase = true;
        logger.debug("ignore case = true");
        itemTokens[0] = itemTokens[0].replaceAll("@", ""); // strip flag
        logger.debug("itemTokens[0]:" + itemTokens[0]);
      }

      Object value = doc.get(new Text(itemTokens[0]));
      if (value != null) // if the field is not present, a null Writable is returned
      {

        if (itemTokens[1].startsWith("[")) // Inclusive range query
        {
          if (value instanceof Text)
          {
            if (!checkRangeQuery(true, itemTokens[0], itemTokens[1], value.toString(), dataSchema))
            {
              logger.debug("checkRangeQuery returned false");
              satisfiesQuery = false;
            }
          }
          else if (value instanceof WritableArrayWritable)
          {
            String[] elements = ((WritableArrayWritable) value).toStrings();
            boolean oneSatisfied = false;
            for (String element : elements)
            {
              if (checkRangeQuery(true, itemTokens[0], itemTokens[1], element, dataSchema))
              {
                logger.debug("checkRangeQuery returned true");
                oneSatisfied = true;
                break;
              }
            }
            satisfiesQuery = oneSatisfied;
          }
        }
        else if (itemTokens[1].startsWith("{")) // Exclusive range query
        {
          if (value instanceof Text)
          {
            if (!checkRangeQuery(false, itemTokens[0], itemTokens[1], value.toString(), dataSchema))
            {
              logger.debug("checkRangeQuery returned false");
              satisfiesQuery = false;
            }
          }
          else if (value instanceof WritableArrayWritable)
          {
            String[] elements = ((WritableArrayWritable) value).toStrings();
            boolean oneSatisfied = false;
            for (String element : elements)
            {
              if (checkRangeQuery(false, itemTokens[0], itemTokens[1], element, dataSchema))
              {
                logger.debug("checkRangeQuery returned true");
                oneSatisfied = true;
                break;
              }
            }
            satisfiesQuery = oneSatisfied;
          }
        }
        else
        // Not a range query
        {
          if (value instanceof Text)
          {
            String valueString = value.toString();
            if (ignoreCase)
            { // Case insensitivity
              logger.debug("not a range query; itemstoken1:" + itemTokens[1]);
              itemTokens[1] = itemTokens[1].toLowerCase();
              valueString = valueString.toLowerCase();
              logger.debug("valuestring after:" + valueString);
            }

            if (itemTokens[1].contains("*") || itemTokens[1].contains("?")) // Wildcard match
            {
              logger.debug("itemTokens[1] = " + itemTokens[1] + " contains wildcard");
              if (!Pattern.matches(wildcardToRegex(itemTokens[1]), valueString))
              {
                logger.debug("stringValue = " + valueString + " did not satisfy itemTokens[1] = " + itemTokens[1]);
                satisfiesQuery = false;
              }
              logger.debug("stringValue = " + valueString + " did satisfy itemTokens[1] = " + itemTokens[1]);
            }
            else if (!(valueString).equals(itemTokens[1])) // Single value match
            {
              logger.debug("We do not have a single value match: stringValue " + valueString + " != itemTokens[1] = " + itemTokens[1]);
              satisfiesQuery = false;
            }
          }
          else if (value instanceof WritableArrayWritable)
          {
            String[] elements = ((WritableArrayWritable) value).toStrings();
            logger.debug("elements.size() = " + elements.length);

            boolean oneSatisfied = false;
            for (String element : elements)
            {
              if (ignoreCase)
              { // Case insensitivity
                itemTokens[1] = itemTokens[1].toLowerCase();
                logger.debug("waw: itemtoken1 after:" + itemTokens[1]);
                element = element.toLowerCase();
                logger.debug("element after:" + element);
              }

              logger.debug("element: " + element);
              if (itemTokens[1].contains("*") || itemTokens[1].contains("?")) // Wildcard match
              {
                logger.debug("itemTokens[1] = " + itemTokens[1] + " contains wildcard");
                if (Pattern.matches(wildcardToRegex(itemTokens[1]), element))
                {
                  logger.debug("stringValue = " + element + " satisfied itemTokens[1] = " + itemTokens[1]);
                  oneSatisfied = true;
                  break;
                }
              }
              else if (element.equals(itemTokens[1])) // Single value match
              {
                logger.debug("We have a single value match: stringValue " + element + " = itemTokens[1] = " + itemTokens[1]);
                oneSatisfied = true;
                break;
              }
            }
            satisfiesQuery = oneSatisfied;
          }
        }
      }
      else
      {
        satisfiesQuery = false; // add fix - account for case if query field does not exist
      }

      ++index; // Try to pick up the boolean operators
      if (index < (queryTokens.length - 1))
      {
        if (queryTokens[index].equals("AND")) // Do nothing and keep going
        {
          if (!satisfiesQuery)
          {
            break;
          }
          ++index;
          item = queryTokens[index];
        }
        else if (queryTokens[index].equals("OR")) // Assume all OR's occur after all AND's
        {
          if (satisfiesQuery) // if we passed the query and it is not the first term
          {
            break;
          }
          else
          {
            ++index;
            item = queryTokens[index];
            satisfiesQuery = true; // reset so that we pick up matches for the next term
          }
        }
        else if (!satisfiesQuery)
        {
          logger.debug("Does not satisfy the query and no boolean ops next...");
          break;
        }
      }
    }

    return satisfiesQuery;
  }

  /**
   * Method to handle ranges queries
   * 
   */
  public static boolean checkRangeQuery(boolean inclusive, String field, String query, String value, DataSchema dataSchema)
  {
    boolean matches = true;

    logger.info("inclusive = " + inclusive + " field = " + field + " query = " + query + " value = " + value);

    // Strip the brackets or braces to obtain query form <lower>+TO+<upper>
    if (inclusive)
    {
      query = query.replaceFirst("\\[", "");
      query = query.replaceFirst("\\]", "");
    }
    else
    {
      query = query.replaceFirst("\\{", "");
      query = query.replaceFirst("\\}", "");
    }
    logger.debug("query = " + query);

    // Special case for IPs
    if (dataSchema.getPartitionerName(field).equals(IPDataPartitioner.class.getName())) // Doesn't handle arrays of IPs in the value right now...
    {
      logger.debug("Have IP Field");

      String[] ranges = query.split("\\+TO\\+");
      logger.info("ranges[0] = " + ranges[0] + " ranges[1] = " + ranges[1]);

      if ((!inclusive) && (value.equals(ranges[0]) || value.equals(ranges[1])))
      {
        logger.debug("inclusive = false and either value.equals(ranges[0]) or value.equals(ranges[1])");
        matches = false;
      }
      else
      {
        String[] blocksLower = ranges[0].split("\\.");
        String[] blocksUpper = ranges[1].split("\\.");
        String[] ipValue = value.split("\\.");
        int ipBlock = 0;
        while (ipBlock < 4)
        {
          logger.info("ipBlock = " + ipBlock + " ipValue[ipBlock] = " + ipValue[ipBlock] + " blocksLower[ipBlock] = " + blocksLower[ipBlock]
              + " blocksUpper[ipBlock] = " + blocksUpper[ipBlock]);

          if (blocksLower[ipBlock].equals(blocksUpper[ipBlock]))
          {
            logger.info("blocksLower[ipBlock].equals(blocksUpper[ipBlock])");
            if (!ipValue[ipBlock].equals(blocksLower[ipBlock]))
            {
              logger.info("!ipValue[ipBlock].equals(blocksLower[ipBlock]");
              matches = false;
            }
          }
          else
          {
            if (!((Integer.parseInt(blocksLower[ipBlock]) <= Integer.parseInt(ipValue[ipBlock])) && (Integer.parseInt(ipValue[ipBlock]) <= Integer
                .parseInt(blocksUpper[ipBlock]))))
            {
              logger.info("IP block not within given range");
              matches = false;
            }
          }
          ++ipBlock;
        }
      }
    }
    else if (field.equals("date"))// Special case for ISO8601 dates & Epoch Dates
    {
      String[] ranges = query.split("\\+TO\\+");
      logger.info("query:" + query);
      logger.info("value:" + value);
      logger.info("ranges[0] = " + ranges[0] + " ranges[1] = " + ranges[1]);

      // code to parse epoch time: VacantArmy
      if ((EpochDateParser.isEpochDateFormat(ranges[0]) || EpochDateParser.isEpochDateSearchFormat(ranges[0]))
          && (EpochDateParser.isEpochDateFormat(ranges[1]) || EpochDateParser.isEpochDateSearchFormat(ranges[1])))
      {
        double fromDate = 0;
        double toDate = 0;
        double valueDate = 0;

        long fromSeconds = 0;
        long fromMilli = 0;
        long toSeconds = 0;
        long toMilli = 0;

        try
        {

          fromDate = EpochDateParser.convertSearchDate(ranges[0]);
          toDate = EpochDateParser.convertSearchDate(ranges[1]);
          valueDate = Double.parseDouble(value);

          logger.debug("fromDate:" + fromDate);
          logger.debug("toDate:" + toDate);
          logger.debug("valueDate:" + valueDate);

          // split up seconds & milliseconds...keep? needed??
          String[] fromDateArr = ranges[0].split("\\.");
          fromSeconds = Integer.parseInt(fromDateArr[0]);
          fromMilli = Integer.parseInt(fromDateArr[1]);

          String[] toDateArr = ranges[1].split("\\.");
          toSeconds = Integer.parseInt(toDateArr[0]);
          toMilli = Integer.parseInt(toDateArr[1]);
        } catch (Exception e)
        {
          logger.info(Arrays.toString(e.getStackTrace()));
        }

        if ((!inclusive) && (fromDate == valueDate || toDate == valueDate))
        {
          logger.debug("(inclusive == false) && (fromDate == valueDate || toDate == valueDate))");
          matches = false;
        }
        else
        {
          if (!((fromDate <= valueDate) && (valueDate <= toDate)))
          {
            logger.debug("valueDate = " + valueDate + " out of range: <" + fromDate + "," + toDate + ">");
            matches = false;
          }
        }
      }
      else
      { // Special case for ISO8061 dates
        long lower = 0;
        long upper = 0;
        long valueDate = 0;
        try
        {
          lower = ISO8601DateParser.getLongDate(ranges[0]);
          upper = ISO8601DateParser.getLongDate(ranges[1]);
          valueDate = ISO8601DateParser.getLongDate(value);
        } catch (ParseException e)
        {
          e.printStackTrace();
        }
        if ((!inclusive) && (lower == valueDate || upper == valueDate))
        {
          logger.debug("(inclusive == false) && (lower == valueDate || upper == valueDate))");
          matches = false;
        }
        else
        {
          if (!((lower <= valueDate) && (valueDate <= upper)))
          {
            logger.debug("valueDate = " + valueDate + " out of range: <" + lower + "," + upper + ">");
            matches = false;
          }
        }
        logger.debug("valueDate  = " + valueDate + " in range: <" + lower + "," + upper + ">");

      }
    }
    else
    // Int
    {

      String[] ranges = query.split("\\+TO\\+");

      // check for ints
      // if (ranges[0].matches("\\d+") && ranges[1].matches("\\d+")){ //int range
      int lower = Integer.parseInt(ranges[0]);
      int upper = Integer.parseInt(ranges[1]);
      int valueInt = Integer.parseInt(value);
      logger.debug("valueInt = " + valueInt + " lower = " + lower + " upper = " + upper);

      if ((!inclusive) && (lower == valueInt || upper == valueInt))
      {
        logger.debug("(inclusive == false) && (lower == valueInt || upper == valueInt))");
        matches = false;
      }
      else
      {
        if (!((lower <= valueInt) && (valueInt <= upper)))
        {
          logger.debug("valueInt = " + valueInt + " out of range: <" + lower + "," + upper + ">");
          matches = false;
        }
      }
      logger.debug("valueInt = " + valueInt + " in range: <" + lower + "," + upper + ">");
    }

    return matches;
  }

  /**
   * Method to convert a URI wildcard query into a java regex
   * 
   */
  public static String wildcardToRegex(String wildcard)
  {
    StringBuilder s = new StringBuilder(wildcard.length());
    for (int i = 0, is = wildcard.length(); i < is; i++)
    {
      char c = wildcard.charAt(i);
      switch (c)
      {
        case '*':
          s.append(".*");
          break;
        case '?':
          s.append(".");
          break;
        // escape special regexp-characters
        case '(':
        case ')':
        case '[':
        case ']':
        case '$':
        case '^':
        case '.':
        case '{':
        case '}':
        case '|':
        case '\\':
          s.append("\\");
          s.append(c);
          break;
        default:
          s.append(c);
          break;
      }
    }
    logger.debug("regex = " + s.toString());

    return (s.toString());
  }
}
