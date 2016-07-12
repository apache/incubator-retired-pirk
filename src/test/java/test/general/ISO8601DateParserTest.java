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
package test.general;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;

import org.apache.log4j.Logger;
import org.apache.pirk.utils.ISO8601DateParser;
import org.apache.pirk.utils.LogUtils;
import org.junit.Test;

/**
 * Class to test basic functionality of ISO8601DateParser class
 */
public class ISO8601DateParserTest
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  @Test
  public void testDateParsing() throws ParseException
  {
    logger.info("Starting testDateParsing: ");

    String date = "2016-02-20T23:29:05.000Z";
    long longDate = Long.parseLong("1456010945000"); //date in UTC
    
    assertEquals(longDate, ISO8601DateParser.getLongDate(date));
    assertEquals(date, ISO8601DateParser.fromLongDate(longDate));

    logger.info("Successfully completed testDateParsing");
  }
}
