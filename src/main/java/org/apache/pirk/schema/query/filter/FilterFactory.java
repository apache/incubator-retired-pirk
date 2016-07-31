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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;

/**
 * Factory class to instantiate filters and set the necessary properties map
 */
public class FilterFactory
{
  public static DataFilter getFilter(String filterName, Set<String> filteredElementNames) throws IOException, PIRException
  {
    Object obj = null;

    if (filterName.equals(StopListFilter.class.getName()))
    {
      FileSystem fs = FileSystem.get(new Configuration());

      // Grab the stopList
      HashSet<String> stopList = new HashSet<>();
      String stopListFile = SystemConfiguration.getProperty("pir.stopListFile", "none");

      if (!stopListFile.equals("none"))
      {
        BufferedReader br;
        if (fs.exists(new Path(stopListFile)))
        {
          br = new BufferedReader(new InputStreamReader(fs.open(new Path(stopListFile))));
        }
        else
        {
          FileReader fr = new FileReader(new File(stopListFile));
          br = new BufferedReader(fr);
        }

        String qLine;
        while ((qLine = br.readLine()) != null)
        {
          stopList.add(qLine);
        }

        obj = new StopListFilter(filteredElementNames, stopList);
      }
    }
    else
    {
      // Instantiate and validate the interface implementation
      try
      {
        @SuppressWarnings("unchecked")
        Class<? extends DataFilter> c = (Class<? extends DataFilter>) Class.forName(filterName);
        obj = c.newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | ClassCastException e)
      {
        throw new PIRException("filterName = " + filterName + " cannot be instantiated or does not implement DataFilter interface");
      }
    }

    return (DataFilter) obj;
  }
}
