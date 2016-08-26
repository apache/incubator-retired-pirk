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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pirk.schema.response.QueryResponseJSON;

public class QueryResultsWriter
{


  /**
   * Writes elements of the resultMap to output file, one line for each element, where each line is a string representation of the corresponding
   * QueryResponseJSON object
   */
  public static void writeResultFile(String filename, Map<String,List<QueryResponseJSON>> resultMap) throws IOException
  {
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filename))))
    {
      for (Entry<String,List<QueryResponseJSON>> entry : resultMap.entrySet())
      {
        for (QueryResponseJSON hitJSON : entry.getValue())
        {
          bw.write(hitJSON.getJSONString());
          bw.newLine();
        }
      }
    }
  }

  /**
   * Writes elements of the resultMap to output file, one line for each element, where each line is a string representation of the corresponding
   * QueryResponseJSON object
   */
  public static void writeResultFile(File file, Map<String,List<QueryResponseJSON>> resultMap) throws IOException
  {
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file)))
    {
      for (Entry<String,List<QueryResponseJSON>> entry : resultMap.entrySet())
      {
        for (QueryResponseJSON hitJSON : entry.getValue())
        {
          bw.write(hitJSON.getJSONString());
          bw.newLine();
        }
      }
    }
  }

}
