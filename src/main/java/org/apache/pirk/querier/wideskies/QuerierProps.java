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

import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

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

  static final List<String> PROPSLIST = Arrays.asList(ACTION, INPUTFILE, OUTPUTFILE, QUERYTYPE, NUMTHREADS, EMBEDQUERYSCHEMA, HASHBITSIZE,
      DATAPARTITIONSIZE, PAILLIERBITSIZE, BITSET, CERTAINTY, QUERYSCHEMAS, DATASCHEMAS, EMBEDSELECTOR, USEMEMLOOKUPTABLE, USEHDFSLOOKUPTABLE, SR_ALGORITHM,
      SR_PROVIDER);

  public static boolean validateQuerierProperties()
  {
    setGeneralDefaults(SystemConfiguration.getProperties());
    if (validateGeneralQuerierProperties(SystemConfiguration.getProperties()))
    {
      String action = SystemConfiguration.getProperty(ACTION).toLowerCase();
      // Action is either "encrypt" or "decrypt", or else we can't get here.
      if (action.equals("encrypt"))
      {
        setEncryptionDefaults(SystemConfiguration.getProperties());
        return validateQuerierEncryptionProperties(SystemConfiguration.getProperties());
      }
      else
      {
        return validateQuerierDecryptionProperties(SystemConfiguration.getProperties());
      }
    }
    else
    {
      return false;
    }
  }

  static void setGeneralDefaults(Properties properties)
  {
    if (!properties.containsKey(EMBEDQUERYSCHEMA))
    {
      properties.setProperty(EMBEDQUERYSCHEMA, "true");
    }
    if (!properties.containsKey(NUMTHREADS))
    {
      properties.setProperty(NUMTHREADS, String.valueOf(Runtime.getRuntime().availableProcessors()));
    }
  }

  public static boolean validateGeneralQuerierProperties(Properties properties)
  {
    boolean valid = true;

    // Parse general required properties

    if (!properties.containsKey(ACTION))
    {
      logger.info("Must have the option " + ACTION);
      valid = false;
    }
    String action = properties.getProperty(ACTION).toLowerCase();
    if (!action.equals("encrypt") && !action.equals("decrypt"))
    {
      logger.info("Unsupported action: " + action);
      valid = false;
    }

    if (!properties.containsKey(INPUTFILE))
    {
      logger.info("Must have the option " + INPUTFILE);
      valid = false;
    }

    if (!properties.containsKey(OUTPUTFILE))
    {
      logger.info("Must have the option " + OUTPUTFILE);
      valid = false;
    }

    if (!properties.containsKey(NUMTHREADS))
    {
      logger.info("Must have the option " + NUMTHREADS);
      valid = false;
    }

    return valid;
  }

  static void setEncryptionDefaults(Properties properties)
  {
    if (!properties.containsKey(EMBEDSELECTOR))
    {
      properties.setProperty(EMBEDSELECTOR, "true");
    }

    if (!properties.containsKey(USEMEMLOOKUPTABLE))
    {
      properties.setProperty(USEMEMLOOKUPTABLE, "false");
    }

    if (!properties.containsKey(USEHDFSLOOKUPTABLE))
    {
      properties.setProperty(USEHDFSLOOKUPTABLE, "false");
    }

    if (!properties.containsKey(BITSET))
    {
      properties.setProperty(BITSET, "-1");
    }
  }

  public static boolean validateQuerierEncryptionProperties(Properties properties)
  {
    boolean valid = true;

    // Parse encryption properties
    if (!properties.containsKey(QUERYTYPE))
    {
      logger.info("For action='encrypt': Must have the option " + QUERYTYPE);
      valid = false;
    }

    if (!properties.containsKey(HASHBITSIZE))
    {
      logger.info("For action='encrypt': Must have the option " + HASHBITSIZE);
      valid = false;
    }

    if (!properties.containsKey(DATAPARTITIONSIZE))
    {
      logger.info("For action='encrypt': Must have the option " + DATAPARTITIONSIZE);
      valid = false;
    }

    if (!properties.containsKey(PAILLIERBITSIZE))
    {
      logger.info("For action='encrypt': Must have the option " + PAILLIERBITSIZE);
      valid = false;
    }

    if (!properties.containsKey(CERTAINTY))
    {
      logger.info("For action='encrypt': Must have the option " + CERTAINTY);
      valid = false;
    }

    if (properties.containsKey(QUERYSCHEMAS))
    {
      appendProperty(properties, "query.schemas", properties.getProperty(QUERYSCHEMAS));
    }

    if (properties.containsKey(DATASCHEMAS))
    {
      appendProperty(properties, "data.schemas", properties.getProperty(DATASCHEMAS));
    }

    return valid;
  }

  public static boolean validateQuerierDecryptionProperties(Properties properties)
  {
    boolean valid = true;

    // Parse decryption args
    if (!properties.containsKey(QUERIERFILE))
    {
      logger.info("For action='decrypt': Must have the option " + QUERIERFILE);
      valid = false;
    }

    return valid;
  }

  private static void appendProperty(Properties properties, String propertyName, String value)
  {
    String oldValue = properties.getProperty(propertyName);

    if (oldValue != null && !oldValue.equals("none"))
    {
      oldValue += "," + value;
    }
    else
    {
      oldValue = value;
    }
    properties.setProperty(propertyName, oldValue);
  }
}
