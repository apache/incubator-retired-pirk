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
package org.apache.pirk.querier.wideskies;

import org.apache.pirk.querier.wideskies.decrypt.DecryptResponse;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.serialization.LocalFileSystemStore;
import org.apache.pirk.utils.FileIOUtils;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.QueryResultsWriter;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Driver class for encryption of a query or decryption of a response
 * <p>
 * File based: For encryption, specify:
 * <p>
 * (1) A query file - one selector per line and the first line in file is the unique query number,
 * <p>
 * (2) The query type, and
 * <p>
 * (3) All necessary encryption parameters.
 * <p>
 * Two corresponding files will be emitted:
 * <p>
 * (1) A file containing the serialized Query object (to be sent to the responder) and
 * <p>
 * (2) A file containing the serialized Querier object to be used for decryption.
 * <p>
 * For decryption, specify:
 * <p>
 * (1) A response file containing the serialized Response object and
 * <p>
 * (2) The corresponding decryption information file containing the serialized Querier object.
 * <p>
 * The output will be a file containing the hits for the query, where each line corresponds to one hit and is the string representation of the corresponding
 * QueryResponseJSON object.
 * <p>
 * Can optionally specify a bit position that must be set in the Paillier modulus
 * <p>
 * TODO:
 * <p>
 * - Add interior functionality for multiple query looping?
 * <p>
 * - Partition size is (practically) fixed at 8 bits, for now... configurable, but ignored right now
 */
public class QuerierDriver implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(QuerierDriver.class);

  public static void main(String... args) throws IOException, InterruptedException, PIRException
  {
    // General variables
    String action;
    String inputFile;
    String outputFile;
    int numThreads;
    LocalFileSystemStore storage = new LocalFileSystemStore();

    // Encryption variables
    int hashBitSize;
    int dataPartitionBitSize;
    int paillierBitSize;
    int certainty;

    // Decryption variables
    String querierFile;

    // Parse the args
    QuerierCLI qdriverCLI = new QuerierCLI(args);

    // Set the variables
    action = SystemConfiguration.getProperty(QuerierProps.ACTION);
    inputFile = SystemConfiguration.getProperty(QuerierProps.INPUTFILE);
    outputFile = SystemConfiguration.getProperty(QuerierProps.OUTPUTFILE);
    numThreads = Integer.parseInt(SystemConfiguration.getProperty(QuerierProps.NUMTHREADS));
    if (action.equals("encrypt"))
    {
      hashBitSize = Integer.parseInt(SystemConfiguration.getProperty(QuerierProps.HASHBITSIZE));
      dataPartitionBitSize = Integer.parseInt(SystemConfiguration.getProperty(QuerierProps.DATAPARTITIONSIZE));
      paillierBitSize = Integer.parseInt(SystemConfiguration.getProperty(QuerierProps.PAILLIERBITSIZE));
      certainty = Integer.parseInt(SystemConfiguration.getProperty(QuerierProps.CERTAINTY));

      logger.info("Performing encryption: \n inputFile = " + inputFile + "\n outputFile = " + outputFile + "\n numThreads = " + numThreads + "\n hashBitSize = "
          + hashBitSize + "\n dataPartitionBitSize = " + dataPartitionBitSize + "\n paillierBitSize = " + paillierBitSize
          + "\n certainty = " + certainty);

      // Read in the selectors and extract the queryIdentifier - first line in the file
      ArrayList<String> selectors = FileIOUtils.readToArrayList(inputFile);
      UUID queryIdentifier = UUID.fromString(selectors.get(0));
      selectors.remove(0);

      int numSelectors = selectors.size();
      logger.info("queryIdentifier = " + queryIdentifier + " numSelectors = " + numSelectors);

      Querier querier = QuerierFactory.createQuerier(queryIdentifier, selectors, SystemConfiguration.getProperties());

      // Write necessary output files - two files written -
      // (1) Querier object to <outputFile>-QuerierConst.QUERIER_FILETAG
      // (2) Query object to <outputFile>-QuerierConst.QUERY_FILETAG
      storage.store(outputFile + "-" + QuerierConst.QUERIER_FILETAG, querier);
      storage.store(outputFile + "-" + QuerierConst.QUERY_FILETAG, querier.getQuery());
    }
    else if (action.equals("decrypt"))
    {
      // Decryption
      querierFile = SystemConfiguration.getProperty(QuerierProps.QUERIERFILE);
      // Reconstruct the necessary objects from the files
      Response response = storage.recall(inputFile, Response.class);
      Querier querier = storage.recall(querierFile, Querier.class);

      UUID querierQueryID = querier.getQuery().getQueryInfo().getIdentifier();
      UUID responseQueryID = response.getQueryInfo().getIdentifier();
      if (!querierQueryID.equals(responseQueryID))
      {
        logger.error("The query identifier in the Response: " + responseQueryID.toString() + " does not match the query identifier specified in the Querier: "
            + querierQueryID.toString());
        System.exit(0);
      }

      // Perform decryption and output the result file
      DecryptResponse decryptResponse = new DecryptResponse(response, querier);
      QueryResultsWriter.writeResultFile(outputFile, decryptResponse.decrypt(numThreads));
    }
  }
}
