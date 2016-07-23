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

import java.util.HashSet;

/**
 * Utilities for stop listing data items/elements
 */
public class StopListUtils
{
  /**
   * Checks to see whether an element (or subdomain of the given element) is contained in the HashSet If it is not in the set, returns true (keep)
   */
  public static boolean checkElement(String element, HashSet<String> filterSet)
  {
    boolean notInSet = true;

    if (filterSet.contains(element))
    {
      notInSet = false;
    }
    else
    {
      String[] tokens = element.split("\\.");
      if (tokens.length > 1)
      {
        String subdomain = element;
        int i = 0;
        while (i < tokens.length - 1)
        {
          subdomain = subdomain.substring(subdomain.indexOf(".") + 1);
          if (filterSet.contains(subdomain))
          {
            notInSet = false;
            break;
          }
          ++i;
        }
      }
    }
    return notInSet;
  }
}
