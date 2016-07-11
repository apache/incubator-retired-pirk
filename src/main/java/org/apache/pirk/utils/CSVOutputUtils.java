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

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Utils class for CSV value output
 * <p>
 * If setting the optional generic String value, can embed a list by encapsulating in quotations: i.e., "thing1,thing2,thing3"
 * 
 */
public class CSVOutputUtils
{
  public static String EMPTYFIELD = "";

  private static Logger logger = LogUtils.getLoggerForThisClass();

  public static Text setCSVOutput(String domain, String ip, String timestamp)
  {
    Text value = new Text();

    String csvOut = domain + "," + ip + "," + timestamp;
    value.set(csvOut);

    return value;
  }

  public static Text setCSVOutput(String domain, String ip, String timestamp, String generic)
  {
    Text value = new Text();

    String csvOut = domain + "," + ip + "," + timestamp + "," + generic;

    value.set(csvOut);

    return value;
  }

  public static void setCSVOutput(Text value, String domain, String ip, String timestamp)
  {
    String csvOut = domain + "," + ip + "," + timestamp;
    value.set(csvOut);
  }

  public static void setCSVOutput(Text value, String domain, String ip, String timestamp, String generic)
  {
    String csvOut = domain + "," + ip + "," + timestamp + "," + generic;

    value.set(csvOut);
  }

  public static String[] extractCSVOutput(Text value)
  {
    String csvOut = value.toString();
    String tokens[] = csvOut.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    return tokens;
  }

  public static String[] extractCSVOutput(String value)
  {
    String tokens[] = value.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    return tokens;
  }

  public static void extractCSVOutputIdentity(Text key, Text value, Text input)
  {
    String csvOut = input.toString();
    String tokens[] = csvOut.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

    key.set(tokens[0]);
    if (tokens.length > 4)
    {
      setCSVOutput(value, tokens[1], tokens[2], tokens[3], tokens[4]);
    }
    else
    {
      setCSVOutput(value, tokens[1], tokens[2], tokens[3]);
    }
  }

  public static void extractCSVOutputIdentityStripFirstField(Text value, Text input)
  {
    String csvOut = input.toString();
    String tokens[] = csvOut.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

    if (tokens.length > 4)
    {
      setCSVOutput(value, tokens[1], tokens[2], tokens[3], tokens[4]);
    }
    else if (tokens.length == 4)
    {
      setCSVOutput(value, tokens[1], tokens[2], tokens[3]);
    }
    else
    {
      logger.info("WARN: tokens.length = " + tokens.length + " != 4 for input = " + csvOut);
      value.set(input.toString());
    }
  }

  public static String extractCSVOutputDomain(Text value)
  {
    String csvOut = value.toString();
    String tokens[] = csvOut.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

    return tokens[0];
  }

  public static String extractCSVOutputIP(Text value)
  {
    String csvOut = value.toString();
    String tokens[] = csvOut.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

    return tokens[1];
  }

  public static String extractCSVOutputTimestamp(Text value)
  {
    String csvOut = value.toString();
    String tokens[] = csvOut.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

    return tokens[2];
  }

  /**
   * Used for testing
   */
  public static String extractCSVOutputByFieldNum(String csvOut, int fieldNum)
  {
    String tokens[] = csvOut.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

    return tokens[fieldNum];
  }

  /**
   * Used for testing -- input is a full csv line: <freq,domain,ip,ts,opt:generic>
   */
  public static String extractCSVOutputLineDomain(String csvOut)
  {
    String tokens[] = csvOut.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

    return tokens[1];
  }

  /**
   * Used for testing -- input is a full csv line: <freq,domain,ip,ts,opt:generic>
   */
  public static String extractCSVOutputLineFreq(String csvOut)
  {
    String tokens[] = csvOut.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

    return tokens[0];
  }

  /**
   * Used for testing
   */
  public static String setFullCSVLine(int frequency, String domain, String ip, String timestamp)
  {
    return frequency + "," + domain + "," + ip + "," + timestamp;
  }

  /**
   * Used for testing
   */
  public static String setFullCSVLine(int frequency, String domain, String ip, String timestamp, String generic)
  {
    return frequency + "," + domain + "," + ip + "," + timestamp + "," + generic;
  }

  /**
   * Used for testing
   */
  public static String setFullCSVLine(String frequency, String domain, String ip, String timestamp)
  {
    return frequency + "," + domain + "," + ip + "," + timestamp;
  }
}
