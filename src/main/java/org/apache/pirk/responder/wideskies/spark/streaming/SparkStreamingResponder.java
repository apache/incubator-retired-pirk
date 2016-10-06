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
package org.apache.pirk.responder.wideskies.spark.streaming;

import java.io.IOException;
import java.security.Permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.pirk.responder.wideskies.spi.ResponderPlugin;
import org.apache.pirk.utils.PIRException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to launch stand alone responder
 */
public class SparkStreamingResponder implements ResponderPlugin
{
  private static final Logger logger = LoggerFactory.getLogger(SparkStreamingResponder.class);

  @Override
  public String getPlatformName()
  {
    return "sparkstreaming";
  }

  @Override
  public void run() throws PIRException
  {
    // For handling System.exit calls from Spark Streaming
    System.setSecurityManager(new SystemExitManager());

    FileSystem fileSys;
    try
    {
      fileSys = FileSystem.get(new Configuration());
    } catch (IOException e)
    {
      throw new PIRException(e);
    }

    logger.info("Launching Spark ComputeStreamingResponse:");
    ComputeStreamingResponse computeSR = null;
    try
    {
      computeSR = new ComputeStreamingResponse(fileSys);
      computeSR.performQuery();
    } catch (SystemExitException e)
    {
      // If System.exit(0) is not caught from Spark Streaming,
      // the application will complete with a 'failed' status
      logger.info("Exited with System.exit(0) from Spark Streaming");
    } catch (IOException e)
    {
      throw new PIRException(e);
    } finally
    {
      // Teardown the context
      if (computeSR != null)
        computeSR.teardown();
    }
  }

  // Exception and Security Manager classes used to catch System.exit from Spark Streaming
  private static class SystemExitException extends SecurityException
  {
    private static final long serialVersionUID = 1L;
  }

  private static class SystemExitManager extends SecurityManager
  {
    @Override
    public void checkPermission(Permission perm)
    {}

    @Override
    public void checkExit(int status)
    {
      super.checkExit(status);
      if (status == 0) // If we exited cleanly, throw SystemExitException
      {
        throw new SystemExitException();
      }
      else
      {
        throw new SecurityException();
      }
    }
  }
}
