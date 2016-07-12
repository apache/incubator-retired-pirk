/*******************************************************************************
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
 *******************************************************************************/
package org.apache.pirk.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;

/**
 * Class to parse a date in ISO86091 format
 * 
 */
public class ISO8601DateParser
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  static
  {
    init();
  }

  private static SimpleDateFormat format;

  private static void init()
  {
    format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public static String parseDate(String date)
  {
    try
    {
      return format.parse(date).getTime() + "";
    } catch (Exception e)
    {}

    return null;
  }

  public static Date getDate(String isoDate) throws ParseException
  {
    return format.parse(isoDate);
  }

  public static long getLongDate(String isoDate) throws ParseException
  {
    return format.parse(isoDate).getTime();
  }

  public static String fromLongDate(long dateLongFormat)
  {
    Date date = new Date(dateLongFormat);
    return format.format(date);
  }
}
