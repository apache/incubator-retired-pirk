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

package org.apache.pirk.responder.wideskies.spi;

import org.apache.hadoop.fs.FileSystem;
import org.apache.pirk.querier.wideskies.Querier;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.utils.PIRException;

import java.util.ArrayList;
import java.util.List;

/**
 * Interface which launches a responder
 * <p>
 * Implement this interface to start the execution of a framework responder, the run method will be called via reflection by the ResponderDriver.
 * </p>
 */
public interface ResponderPlugin
{
  /**
   * Returns the plugin name for your framework This will be the platform argument
   * 
   * @return
   */
  public String getPlatformName();

  /**
   * This method launches your framework responder.
   */
  public void run() throws PIRException;

  /**
   * This method specifies if your framework has a distributed test override to return true
   * if you implement runDistributed test.
   * @return
   */
  default boolean hasDistributedTest()
  {
    return false;
  }

  /**
   * This method sets up all the requirements for a responder to run a query,
   * invokes run(), reads the results and returns a List of QueryResponseJSON to
   * be verified by BaseTests.performQuery.
   * @return The results of the QueryResponse
   * @throws PIRException
   */
  default public List<QueryResponseJSON> runDistributedTest(ArrayList<String> args, FileSystem fs, Querier querier, int numThreads) throws PIRException
  {
    throw new PIRException("Distributed Test not implemented for " + getPlatformName());
  }

}
