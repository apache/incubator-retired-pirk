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

import java.util.Arrays;
import java.util.List;

import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.query.QuerySchemaLoader;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Properties constants and validation for the Querier
 */
public class QuerierProps
{
  private static final Logger logger = LoggerFactory.getLogger(QuerierProps.class);

  // General properties
  static final String ACTION = "querier.action";
  static final String INPUTFILE = "querier.inputFile";
  public static final String OUTPUTFILE = "querier.outputFile";
  static final String QUERYTYPE = "querier.queryType";
  static final String NUMTHREADS = "querier.numThreads";

  // Encryption properties
  static final String HASHBITSIZE = "querier.hashBitSize";
  static final String HASHKEY = "querier.hashKey";
  static final String DATAPARTITIONSIZE = "querier.dataPartitionBitSize";
  static final String PAILLIERBITSIZE = "querier.paillierBitSize";
  static final String BITSET = "querier.bitSet";
  static final String CERTAINTY = "querier.certainty";
  static final String QUERYSCHEMAS = "querier.querySchemas";
  static final String DATASCHEMAS = "querier.dataSchemas";
  static final String EMBEDSELECTOR = "querier.embedSelector";
  static final String USEMEMLOOKUPTABLE = "querier.memLookupTable";
  static final String USEHDFSLOOKUPTABLE = "querier.useHDFSLookupTable";
  static final String SR_ALGORITHM = "pallier.secureRandom.algorithm";
  static final String SR_PROVIDER = "pallier.secureRandom.provider";
  static final String EMBEDQUERYSCHEMA = "pir.embedQuerySchema";

  // Decryption properties
  static final String QUERIERFILE = "querier.querierFile";

  static final List<String> PROPSLIST = Arrays.asList(ACTION, INPUTFILE, OUTPUTFILE, QUERYTYPE, NUMTHREADS, EMBEDQUERYSCHEMA, HASHBITSIZE, HASHKEY,
      DATAPARTITIONSIZE, PAILLIERBITSIZE, BITSET, CERTAINTY, QUERYSCHEMAS, DATASCHEMAS, EMBEDSELECTOR, USEMEMLOOKUPTABLE, USEHDFSLOOKUPTABLE, SR_ALGORITHM,
      SR_PROVIDER);

  /**
   * Validates the querier properties
   *
   */
  public static boolean validateQuerierProperties()
  {
    boolean valid = true;

    // Parse general required properties

    if (!SystemConfiguration.hasProperty(ACTION))
    {
      logger.info("Must have the option " + ACTION);
      valid = false;
    }
    String action = SystemConfiguration.getProperty(ACTION).toLowerCase();
    if (!action.equals("encrypt") && !action.equals("decrypt"))
    {
      logger.info("Unsupported action: " + action);
      valid = false;
    }

    if (!SystemConfiguration.hasProperty(INPUTFILE))
    {
      logger.info("Must have the option " + INPUTFILE);
      valid = false;
    }

    if (!SystemConfiguration.hasProperty(OUTPUTFILE))
    {
      logger.info("Must have the option " + OUTPUTFILE);
      valid = false;
    }

    if (!SystemConfiguration.hasProperty(NUMTHREADS))
    {
      logger.info("Must have the option " + NUMTHREADS);
      valid = false;
    }

    // Parse general optional properties
    if (!SystemConfiguration.hasProperty(EMBEDQUERYSCHEMA))
    {
      SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
    }

    // Parse encryption properties

    if (action.equals("encrypt"))
    {
      if (!SystemConfiguration.hasProperty(QUERYTYPE))
      {
        logger.info("For action='encrypt': Must have the option " + QUERYTYPE);
        valid = false;
      }

      if (!SystemConfiguration.hasProperty(HASHBITSIZE))
      {
        logger.info("For action='encrypt': Must have the option " + HASHBITSIZE);
        valid = false;
      }

      if (!SystemConfiguration.hasProperty(HASHKEY))
      {
        logger.info("For action='encrypt': Must have the option " + HASHKEY);
        valid = false;
      }

      if (!SystemConfiguration.hasProperty(DATAPARTITIONSIZE))
      {
        logger.info("For action='encrypt': Must have the option " + DATAPARTITIONSIZE);
        valid = false;
      }

      if (!SystemConfiguration.hasProperty(PAILLIERBITSIZE))
      {
        logger.info("For action='encrypt': Must have the option " + PAILLIERBITSIZE);
        valid = false;
      }

      if (!SystemConfiguration.hasProperty(CERTAINTY))
      {
        logger.info("For action='encrypt': Must have the option " + CERTAINTY);
        valid = false;
      }

      if (!SystemConfiguration.hasProperty(BITSET))
      {
        logger.info("For action='encrypt': Must have the option " + BITSET);
        valid = false;
      }

      if (SystemConfiguration.hasProperty(QUERYSCHEMAS))
      {
        SystemConfiguration.appendProperty("query.schemas", SystemConfiguration.getProperty(QUERYSCHEMAS));
      }

      if (SystemConfiguration.hasProperty(DATASCHEMAS))
      {
        SystemConfiguration.appendProperty("data.schemas", SystemConfiguration.getProperty(DATASCHEMAS));
      }

      if (!SystemConfiguration.hasProperty(EMBEDSELECTOR))
      {
        SystemConfiguration.setProperty(EMBEDSELECTOR, "true");
      }

      if (!SystemConfiguration.hasProperty(USEMEMLOOKUPTABLE))
      {
        SystemConfiguration.setProperty(USEMEMLOOKUPTABLE, "false");
      }

      if (!SystemConfiguration.hasProperty(USEHDFSLOOKUPTABLE))
      {
        SystemConfiguration.setProperty(USEHDFSLOOKUPTABLE, "false");
      }
    }

    // Parse decryption args
    if (action.equals("decrypt"))
    {
      if (!SystemConfiguration.hasProperty(QUERIERFILE))
      {
        logger.info("For action='decrypt': Must have the option " + QUERIERFILE);
        valid = false;
      }
    }

    // Load the new local query and data schemas
    if (valid)
    {
      logger.info("loading schemas: dataSchemas = " + SystemConfiguration.getProperty("data.schemas") + " querySchemas = "
          + SystemConfiguration.getProperty("query.schemas"));
      try
      {
        DataSchemaLoader.initialize();
        QuerySchemaLoader.initialize();

      } catch (Exception e)
      {
        e.printStackTrace();
      }
    }

    return valid;
  }
}
