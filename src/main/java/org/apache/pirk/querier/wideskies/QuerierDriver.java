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

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.querier.wideskies.decrypt.DecryptResponse;
import org.apache.pirk.querier.wideskies.encrypt.EncryptQuery;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.apache.pirk.serialization.LocalFileSystemStore;
import org.apache.pirk.utils.FileIOUtils;
import org.apache.pirk.utils.PIRException;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    String queryType = null;
    int numThreads;
    LocalFileSystemStore storage = new LocalFileSystemStore();

    // Encryption variables
    int hashBitSize = 0;
    String hashKey = null;
    int dataPartitionBitSize = 0;
    int paillierBitSize = 0;
    int certainty = 0;
    String queryName = null;
    int bitSet = -1;
    boolean embedSelector = true;
    boolean useMemLookupTable = false;
    boolean useHDFSLookupTable = false;

    // Decryption variables
    String querierFile = null;

    // Parse the args
    QuerierDriverCLI qdriverCLI = new QuerierDriverCLI(args);

    // Set the variables
    action = qdriverCLI.getOptionValue(QuerierDriverCLI.ACTION);
    inputFile = qdriverCLI.getOptionValue(QuerierDriverCLI.INPUTFILE);
    outputFile = qdriverCLI.getOptionValue(QuerierDriverCLI.OUTPUTFILE);
    numThreads = Integer.parseInt(qdriverCLI.getOptionValue(QuerierDriverCLI.NUMTHREADS));
    if (action.equals("encrypt"))
    {
      queryType = qdriverCLI.getOptionValue(QuerierDriverCLI.TYPE);
      queryName = qdriverCLI.getOptionValue(QuerierDriverCLI.QUERYNAME);
      hashBitSize = Integer.parseInt(qdriverCLI.getOptionValue(QuerierDriverCLI.HASHBITSIZE));
      hashKey = qdriverCLI.getOptionValue(QuerierDriverCLI.HASHBITSIZE);
      dataPartitionBitSize = Integer.parseInt(qdriverCLI.getOptionValue(QuerierDriverCLI.DATAPARTITIONSIZE));
      paillierBitSize = Integer.parseInt(qdriverCLI.getOptionValue(QuerierDriverCLI.PAILLIERBITSIZE));
      certainty = Integer.parseInt(qdriverCLI.getOptionValue(QuerierDriverCLI.CERTAINTY));
      embedSelector = SystemConfiguration.getProperty(QuerierDriverCLI.EMBEDSELECTOR, "true").equals("true");
      useMemLookupTable = SystemConfiguration.getProperty(QuerierDriverCLI.USEMEMLOOKUPTABLE, "false").equals("true");
      useHDFSLookupTable = SystemConfiguration.getProperty(QuerierDriverCLI.USEHDFSLOOKUPTABLE, "false").equals("true");

      if (qdriverCLI.hasOption(QuerierDriverCLI.BITSET))
      {
        bitSet = Integer.parseInt(qdriverCLI.getOptionValue(QuerierDriverCLI.BITSET));
        logger.info("bitSet = " + bitSet);
      }

      // Check to ensure we have a valid queryType
      if (!LoadQuerySchemas.containsSchema(queryType))
      {
        logger.error("Invalid schema: " + queryType + "; The following schemas are loaded:");
        LoadQuerySchemas.printSchemas();
        System.exit(0);
      }

      // Enforce dataPartitionBitSize < 32
      if (dataPartitionBitSize > 31)
      {
        logger.error("dataPartitionBitSize = " + dataPartitionBitSize + "; must be less than 32");
      }
    }
    if (action.equals("decrypt"))
    {
      querierFile = qdriverCLI.getOptionValue(QuerierDriverCLI.QUERIERFILE);
    }

    // Perform the action
    if (action.equals("encrypt"))
    {
      logger.info("Performing encryption: \n inputFile = " + inputFile + "\n outputFile = " + outputFile + "\n numThreads = " + numThreads
          + "\n hashBitSize = " + hashBitSize + "\n hashKey = " + hashKey + "\n dataPartitionBitSize = " + dataPartitionBitSize + "\n paillierBitSize = "
          + paillierBitSize + "\n certainty = " + certainty);

      // Read in the selectors and extract the queryNum - first line in the file
      ArrayList<String> selectors = FileIOUtils.readToArrayList(inputFile);
      double queryNum = Double.parseDouble(selectors.get(0));
      selectors.remove(0);

      int numSelectors = selectors.size();
      logger.info("queryNum = " + queryNum + " numSelectors = " + numSelectors);

      // Set the necessary QueryInfo and Paillier objects
      QueryInfo queryInfo = new QueryInfo(queryNum, numSelectors, hashBitSize, hashKey, dataPartitionBitSize, queryType, queryName, paillierBitSize,
          useMemLookupTable, embedSelector, useHDFSLookupTable);

      if (SystemConfiguration.getProperty("pir.embedQuerySchema").equals("true"))
      {
        queryInfo.addQuerySchema(LoadQuerySchemas.getSchema(queryType));
      }

      Paillier paillier = new Paillier(paillierBitSize, certainty, bitSet); // throws PIRException if certainty conditions are not satisfied

      // Check the number of selectors to ensure that 2^{numSelector*dataPartitionBitSize} < N
      // For example, if the highest bit is set, the largest value is \floor{paillierBitSize/dataPartitionBitSize}
      int exp = numSelectors * dataPartitionBitSize;
      BigInteger val = (BigInteger.valueOf(2)).pow(exp);
      if (val.compareTo(paillier.getN()) != -1)
      {
        logger.error("The number of selectors = " + numSelectors + " must be such that " + "2^{numSelector*dataPartitionBitSize} < N = "
            + paillier.getN().toString(2));
        System.exit(0);
      }

      // Perform the encryption
      EncryptQuery encryptQuery = new EncryptQuery(queryInfo, selectors, paillier);
      encryptQuery.encrypt(numThreads);

      // Write necessary output files - two files written -
      // (1) Querier object to <outputFile>-QuerierConst.QUERIER_FILETAG
      // (2) Query object to <outputFile>-QuerierConst.QUERY_FILETAG
      storage.store(outputFile + "-" + QuerierConst.QUERIER_FILETAG, encryptQuery.getQuerier());
      storage.store(outputFile + "-" + QuerierConst.QUERY_FILETAG, encryptQuery.getQuery());
    }
    else
    // Decryption
    {
      // Reconstruct the necessary objects from the files
      Response response = storage.recall(inputFile, Response.class);
      Querier querier = storage.recall(querierFile, Querier.class);

      // Perform decryption and output the result file
      DecryptResponse decryptResponse = new DecryptResponse(response, querier);
      decryptResponse.decrypt(numThreads);
      decryptResponse.writeResultFile(outputFile);
    }
  }
}
