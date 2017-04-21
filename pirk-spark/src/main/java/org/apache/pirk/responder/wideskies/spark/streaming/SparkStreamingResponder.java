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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pirk.querier.wideskies.Querier;
import org.apache.pirk.querier.wideskies.decrypt.DecryptResponse;
import org.apache.pirk.responder.wideskies.ResponderProps;
import org.apache.pirk.responder.wideskies.spi.ResponderPlugin;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.serialization.HadoopFileSystemStore;
import org.apache.pirk.test.distributed.DistributedTestDriver;
import org.apache.pirk.test.utils.TestUtils;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.QueryResultsWriter;
import org.apache.pirk.utils.SystemConfiguration;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to launch stand alone responder
 */
public class SparkStreamingResponder implements ResponderPlugin
{

  private static final Logger logger = LoggerFactory.getLogger(SparkStreamingResponder.class);

  protected void deleteOutput(String outputFile, FileSystem fs) throws PIRException
  {
    try
    {
      Path path = new Path(outputFile);
      if (fs.exists(path))
        fs.delete(path, true); // Ensure old output does not exist.
    } catch (IOException e)
    {
      throw new PIRException("Failed to delete output file " + outputFile + ". " + e.getMessage());
    }
  }

  protected Response getResponse(String outputFile, FileSystem fs) throws PIRException
  {
    try
    {
      return new HadoopFileSystemStore(fs).recall(outputFile, Response.class);
    } catch (IOException e)
    {
      throw new PIRException("getResponse: Failed to open output file " + outputFile + ". " + e.getMessage());
    }
  }

  protected File getTempFile() throws PIRException
  {
    try
    {
      return File.createTempFile("finalResultsFile", ".txt");
    } catch (IOException e)
    {
      throw new PIRException("Failed to open temp file: finalResultsFile.txt." + e.getMessage());
    }
  }

  protected void writeDecryptedResults(DecryptResponse decryptResponse, File output, int numThreads) throws PIRException
  {
    try
    {
      QueryResultsWriter.writeResultFile(output, decryptResponse.decrypt(numThreads));
    } catch (InterruptedException e)
    {
      throw new PIRException("Failed to decrypt responses, Thread interupted." + e.getMessage());
    } catch (IOException e)
    {
      throw new PIRException("Failed to write temp file: finalResultsFile.txt" + e.getMessage());
    }
    logger.info("Completed performing decryption and writing final results file");
  }

  protected List<QueryResponseJSON> readResults(File input) throws PIRException
  {
    logger.info("Reading in and checking results");
    try
    {
      return TestUtils.readResultsFile(input);
    } catch (IOException e)
    {
      throw new PIRException("Failed to read temp file: finalResultsFile.txt" + e.getMessage());
    }
  }

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

  @Override
  public boolean hasDistributedTest()
  {
    return true;
  }

  @Override
  public List<QueryResponseJSON> runDistributedTest(ArrayList<String> args, FileSystem fs, Querier querier, int numThreads) throws PIRException
  {
    logger.info("platform = sparkstreaming");
    String outputFile = SystemConfiguration.getProperty(DistributedTestDriver.OUTPUT_DIRECTORY_PROPERTY);
    args.add("-" + ResponderProps.PLATFORM + "=sparkstreaming");
    args.add("-" + ResponderProps.BATCHSECONDS + "=" + SystemConfiguration.getProperty("pir.sparkstreaming.batchSeconds", "30"));
    args.add("-" + ResponderProps.WINDOWLENGTH + "=" + SystemConfiguration.getProperty("pir.sparkstreaming.windowLength", "60"));
    args.add("-" + ResponderProps.MAXBATCHES + "=" + SystemConfiguration.getProperty("pir.sparkstreaming.maxBatches", "1"));
    args.add("-" + ResponderProps.STOPGRACEFULLY + "=" + SystemConfiguration.getProperty("spark.streaming.stopGracefullyOnShutdown", "false"));
    args.add("-" + ResponderProps.NUMDATAPARTITIONS + "=" + SystemConfiguration.getProperty("pir.numDataPartitions", "3"));
    args.add("-" + ResponderProps.USEQUEUESTREAM + "=" + SystemConfiguration.getProperty("pir.sparkstreaming.useQueueStream", "true"));

    logger.info("Pulling results from outputFile = " + outputFile);
    deleteOutput(outputFile, fs);

    try
    {
      Process sLauncher = new SparkLauncher().setAppResource(SystemConfiguration.getProperty("jarFile"))
          .setSparkHome(SystemConfiguration.getProperty("spark.home"))
          .setMainClass("org.apache.pirk.responder.wideskies.ResponderDriver").addAppArgs(args.toArray(new String[args.size()])).setMaster("yarn-cluster")
          .setConf(SparkLauncher.EXECUTOR_MEMORY, "2g").setConf(SparkLauncher.DRIVER_MEMORY, "2g").setConf(SparkLauncher.EXECUTOR_CORES, "1").launch();
      BufferedReader errorReader = new BufferedReader(new InputStreamReader(sLauncher.getErrorStream()));
      String line;
      while((line = errorReader.readLine()) != null) {
        logger.info(line);
      }
      sLauncher.waitFor();
    } catch (IOException e)
    {
      throw new PIRException("Spark Job did not finish" + e.getMessage());
    } catch (InterruptedException e)
    {
      throw new PIRException("Spark Job did not finish" + e.getMessage());
    }

    // Perform the Query
    File fileFinalResults = getTempFile();
    fileFinalResults.deleteOnExit();
    logger.info("fileFinalResults = " + fileFinalResults.getAbsolutePath());
    fileFinalResults.deleteOnExit();
    logger.info("fileFinalResults = " + fileFinalResults.getAbsolutePath());
    writeDecryptedResults(new DecryptResponse(getResponse(outputFile + "_0", fs), querier), fileFinalResults, numThreads);
    return readResults(fileFinalResults);
  }
}
