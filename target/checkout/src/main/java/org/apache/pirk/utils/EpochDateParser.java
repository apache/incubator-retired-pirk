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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to parse dates in Epoch date format
 */
public class EpochDateParser
{

  private static final Logger logger = LoggerFactory.getLogger(EpochDateParser.class);

  public static boolean isEpochDateFormat(String date)
  {
    return (date.matches("\\d+[.]\\d{3}"));
  }

  /*
   * Valid formats for searching (allow wildcard in MILLISECONDS): 2432.*, 2312.2*, 2312.22*, 2312.123
   */
  public static boolean isEpochDateSearchFormat(String date)
  {
    return (date.matches("\\d+[.](\\d+|[*]|\\d+[*])"));
  }

  /* Converts the passed in date to a parseable date format */
  public static double convertSearchDate(String date)
  {
    if (containsWildcard(date))
      return stripWildcard(date);
    else
      return Double.parseDouble(date);
  }

  public static boolean containsWildcard(String date)
  {
    return (date.contains("*"));
  }

  public static double stripWildcard(String date)
  {
    String x = date.replaceAll("[*]", "");
    return Double.parseDouble(x);
  }

  public static boolean isInt(String arg)
  {
    return (arg.matches("\\d+"));
  }

  public static boolean isDouble(String arg)
  {
    return (arg.matches("\\d+[.]\\d+"));
  }

}
