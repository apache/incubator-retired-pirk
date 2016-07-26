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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.pirk.schema.data.LoadDataSchemas;
import org.apache.pirk.schema.query.LoadQuerySchemas;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for parsing the command line options for the QuerierDriver
 */
public class QuerierDriverCLI
{
  private static final Logger logger = LoggerFactory.getLogger(QuerierDriverCLI.class);

  private Options cliOptions = null;
  private CommandLine commandLine = null;

  private static final String LOCALPROPFILE = "local.querier.properties";

  /**
   * Create and parse allowable options
   * 
   */
  public QuerierDriverCLI(String[] args)
  {
    // Create the command line options
    cliOptions = createOptions();

    try
    {
      // Parse the command line options
      CommandLineParser parser = new GnuParser();
      commandLine = parser.parse(cliOptions, args, true);

      // if help option is selected, just print help text and exit
      if (hasOption("h"))
      {
        printHelp();
        System.exit(1);
      }

      // Parse and validate the options provided
      if (!parseOptions())
      {
        logger.info("The provided options are not valid");
        printHelp();
        System.exit(1);
      }
    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Determine if an option was provided by the user via the CLI
   * 
   * @param option
   *          - the option of interest
   * @return true if option was provided, false otherwise
   */
  public boolean hasOption(String option)
  {
    return commandLine.hasOption(option);
  }

  /**
   * Obtain the argument of the option provided by the user via the CLI
   * 
   * @param option
   *          - the option of interest
   * @return value of the argument of the option
   */
  public String getOptionValue(String option)
  {
    return commandLine.getOptionValue(option);
  }

  /**
   * Method to parse and validate the options provided
   * 
   * @return - true if valid, false otherwise
   */
  private boolean parseOptions()
  {
    boolean valid = true; 

    //If we have a local.querier.properties file specified, load it 
    if(hasOption(LOCALPROPFILE))
    {
      SystemConfiguration.loadPropsFromFile(getOptionValue(LOCALPROPFILE));
    }
    else  
    {
      //Pull options, set as properties   
      for(String prop: QuerierProps.PROPSLIST)
      {
        if(hasOption(prop))
        {
          SystemConfiguration.setProperty(prop, getOptionValue(prop));
        }
      }
  }

    //Validate properties
    valid = QuerierProps.validateQuerierProperties();

    // Load the new local query and data schemas
    if(valid)
    {
      logger.info("loading schemas: dataSchemas = " + SystemConfiguration.getProperty("data.schemas") + " querySchemas = "
          + SystemConfiguration.getProperty("query.schemas"));
      try
      {
        LoadDataSchemas.initialize();
        LoadQuerySchemas.initialize();

      } catch (Exception e)
      {
        e.printStackTrace();
      }
    }

    return valid;
  }

  /**
   * Create the options available for the DistributedTestDriver
   * 
   * @return Apache's CLI Options object
   */
  private Options createOptions()
  {
    Options options = new Options();

    // help
    Option optionHelp = new Option("h", "help", false, "Print out the help documentation for this command line execution");
    optionHelp.setRequired(false);
    options.addOption(optionHelp);

    // local.querier.properties
    Option optionLocalPropFile = new Option("localPropFile", LOCALPROPFILE, false, "Optional local properties file");
    optionLocalPropFile.setRequired(false);
    options.addOption(optionLocalPropFile);

    // ACTION
    Option optionACTION = new Option("a", QuerierProps.ACTION, true, "required - 'encrypt' or 'decrypt' -- The action performed by the QuerierDriver");
    optionACTION.setRequired(false);
    optionACTION.setArgName(QuerierProps.ACTION);
    optionACTION.setType(String.class);
    options.addOption(optionACTION);

    // INPUTFILE
    Option optionINPUTFILE = new Option("i", QuerierProps.INPUTFILE, true, "required - Fully qualified file containing input "
        + "-- \n The input is either: \n (1) For Encryption: A query file - Contains the query selectors, one per line; "
        + "the first line must be the query number \n OR \n (2) For Decryption: A response file - Contains the serialized Response object");
    optionINPUTFILE.setRequired(false);
    optionINPUTFILE.setArgName(QuerierProps.INPUTFILE);
    optionINPUTFILE.setType(String.class);
    options.addOption(optionINPUTFILE);

    // OUTPUTFILE
    Option optionOUTPUTFILE = new Option("o", QuerierProps.OUTPUTFILE, true, "required - Fully qualified file for the result output. "
        + "\n The output file specifies either: \n (1) For encryption: \n \t (a) A file to contain the serialized Querier object named: " + "<outputFile>-"
        + QuerierConst.QUERIER_FILETAG + "  AND \n \t " + "(b) A file to contain the serialized Query object named: <outputFile>-" + QuerierConst.QUERY_FILETAG
        + "\n " + "OR \n (2) A file to contain the decryption results where each line is where each line "
        + "corresponds to one hit and is a JSON object with the schema QuerySchema");
    optionOUTPUTFILE.setRequired(false);
    optionOUTPUTFILE.setArgName(QuerierProps.OUTPUTFILE);
    optionOUTPUTFILE.setType(String.class);
    options.addOption(optionOUTPUTFILE);

    // NUMTHREADS
    Option optionNUMTHREADS = new Option("nt", QuerierProps.NUMTHREADS, true, "required -- Number of threads to use for encryption/decryption");
    optionNUMTHREADS.setRequired(false);
    optionNUMTHREADS.setArgName(QuerierProps.NUMTHREADS);
    optionNUMTHREADS.setType(String.class);
    options.addOption(optionNUMTHREADS);

    // DATASCHEMAS
    Option optionDataSchemas = new Option("ds", QuerierProps.DATASCHEMAS, true, "optional -- Comma separated list of data schema file names");
    optionDataSchemas.setRequired(false);
    optionDataSchemas.setArgName(QuerierProps.DATASCHEMAS);
    optionDataSchemas.setType(String.class);
    options.addOption(optionDataSchemas);

    // QUERYSCHEMAS
    Option optionQuerySchemas = new Option("qs", QuerierProps.QUERYSCHEMAS, true, "optional -- Comma separated list of query schema file names");
    optionQuerySchemas.setRequired(false);
    optionQuerySchemas.setArgName(QuerierProps.QUERYSCHEMAS);
    optionQuerySchemas.setType(String.class);
    options.addOption(optionQuerySchemas);

    // TYPE
    Option optionTYPE = new Option("qt", QuerierProps.TYPE, true, "required for encryption -- Type of the query as defined "
        + "in the 'schemaName' tag of the corresponding query schema file");
    optionTYPE.setRequired(false);
    optionTYPE.setArgName(QuerierProps.TYPE);
    optionTYPE.setType(String.class);
    options.addOption(optionTYPE);

    // NAME
    Option optionNAME = new Option("qn", QuerierProps.QUERYNAME, true, "required for encryption -- Name of the query");
    optionNAME.setRequired(false);
    optionNAME.setArgName(QuerierProps.QUERYNAME);
    optionNAME.setType(String.class);
    options.addOption(optionNAME);

    // HASHBITSIZE
    Option optionHASHBITSIZE = new Option("hb", QuerierProps.HASHBITSIZE, true, "required -- Bit size of keyed hash");
    optionHASHBITSIZE.setRequired(false);
    optionHASHBITSIZE.setArgName(QuerierProps.HASHBITSIZE);
    optionHASHBITSIZE.setType(String.class);
    options.addOption(optionHASHBITSIZE);

    // HASHKEY
    Option optionHASHKEY = new Option("hk", QuerierProps.HASHKEY, true, "required for encryption -- String key for the keyed hash functionality");
    optionHASHKEY.setRequired(false);
    optionHASHKEY.setArgName(QuerierProps.HASHKEY);
    optionHASHKEY.setType(String.class);
    options.addOption(optionHASHKEY);

    // DATAPARTITIONSIZE
    Option optionDATAPARTITIONSIZE = new Option("dps", QuerierProps.DATAPARTITIONSIZE, true, "required for encryption -- Partition bit size in data partitioning");
    optionDATAPARTITIONSIZE.setRequired(false);
    optionDATAPARTITIONSIZE.setArgName(QuerierProps.DATAPARTITIONSIZE);
    optionDATAPARTITIONSIZE.setType(String.class);
    options.addOption(optionDATAPARTITIONSIZE);

    // PAILLIERBITSIZE
    Option optionPAILLIERBITSIZE = new Option("pbs", QuerierProps.PAILLIERBITSIZE, true, "required for encryption -- Paillier modulus size N");
    optionPAILLIERBITSIZE.setRequired(false);
    optionPAILLIERBITSIZE.setArgName(QuerierProps.PAILLIERBITSIZE);
    optionPAILLIERBITSIZE.setType(String.class);
    options.addOption(optionPAILLIERBITSIZE);

    // CERTAINTY
    Option optionCERTAINTY = new Option("c", QuerierProps.CERTAINTY, true,
        "required for encryption -- Certainty of prime generation for Paillier -- must  be greater than or " + "equal to "
            + SystemConfiguration.getProperty("pir.primeCertainty") + "");
    optionCERTAINTY.setRequired(false);
    optionCERTAINTY.setArgName(QuerierProps.CERTAINTY);
    optionCERTAINTY.setType(String.class);
    options.addOption(optionCERTAINTY);

    // BITSET
    Option optionBITSET = new Option("b", QuerierProps.BITSET, true, "required for encryption -- Ensure that this bit position is set in the "
        + "Paillier modulus (will generate Paillier moduli until finding one in which this bit is set)");
    optionBITSET.setRequired(false);
    optionBITSET.setArgName(QuerierProps.BITSET);
    optionBITSET.setType(String.class);
    options.addOption(optionBITSET);

    // embedSelector
    Option optionEmbedSelector = new Option("embed", QuerierProps.EMBEDSELECTOR, true, "required for encryption -- 'true' or 'false' - Whether or not to embed "
        + "the selector in the results to reduce false positives");
    optionEmbedSelector.setRequired(false);
    optionEmbedSelector.setArgName(QuerierProps.EMBEDSELECTOR);
    optionEmbedSelector.setType(String.class);
    options.addOption(optionEmbedSelector);

    // useMemLookupTable
    Option optionUseMemLookupTable = new Option("mlu", QuerierProps.USEMEMLOOKUPTABLE, true,
        "required for encryption -- 'true' or 'false' - Whether or not to generate and use "
            + "an in memory modular exponentation lookup table - only for standalone/testing right now...");
    optionUseMemLookupTable.setRequired(false);
    optionUseMemLookupTable.setArgName(QuerierProps.USEMEMLOOKUPTABLE);
    optionUseMemLookupTable.setType(String.class);
    options.addOption(optionUseMemLookupTable);

    // useHDFSLookupTable
    Option optionUseHDFSLookupTable = new Option("lu", QuerierProps.USEHDFSLOOKUPTABLE, true,
        "required for encryption -- 'true' or 'false' -- Whether or not to generate and use " + "a hdfs modular exponentation lookup table");
    optionUseHDFSLookupTable.setRequired(false);
    optionUseHDFSLookupTable.setArgName(QuerierProps.USEHDFSLOOKUPTABLE);
    optionUseHDFSLookupTable.setType(String.class);
    options.addOption(optionUseHDFSLookupTable);

    // QUERIERFILE
    Option optionQUERIERFILE = new Option("qf", QuerierProps.QUERIERFILE, true, "required for decryption -- Fully qualified file containing the serialized Querier object");
    optionQUERIERFILE.setRequired(false);
    optionQUERIERFILE.setArgName(QuerierProps.QUERIERFILE);
    optionQUERIERFILE.setType(String.class);
    options.addOption(optionQUERIERFILE);

    // embedQuerySchema
    Option optionEMBEDQUERYSCHEMA = new Option("embedQS", QuerierProps.EMBEDQUERYSCHEMA, true,
        "optional (defaults to false) -- Whether or not to embed the QuerySchema in the Query (via QueryInfo)");
    optionEMBEDQUERYSCHEMA.setRequired(false);
    optionEMBEDQUERYSCHEMA.setArgName(QuerierProps.EMBEDQUERYSCHEMA);
    optionEMBEDQUERYSCHEMA.setType(String.class);
    options.addOption(optionEMBEDQUERYSCHEMA);

    // SR_ALGORITHM
    Option optionSR_ALGORITHM = new Option("srAlg", QuerierProps.SR_ALGORITHM, true, "optional - specify the SecureRandom algorithm, defaults to NativePRNG");
    optionSR_ALGORITHM.setRequired(false);
    optionSR_ALGORITHM.setArgName(QuerierProps.SR_ALGORITHM);
    optionSR_ALGORITHM.setType(String.class);
    options.addOption(optionSR_ALGORITHM);

    // SR_PROVIDERS
    Option optionSR_PROVIDER = new Option("srProvider", QuerierProps.SR_PROVIDER, true, "optional - specify the SecureRandom provider, defaults to SUN");
    optionSR_PROVIDER.setRequired(false);
    optionSR_PROVIDER.setArgName(QuerierProps.SR_PROVIDER);
    optionSR_PROVIDER.setType(String.class);
    options.addOption(optionSR_PROVIDER);

    return options;
  }

  /**
   * Prints out the help message
   */
  private void printHelp()
  {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(140);
    formatter.printHelp("QuerierDriver", cliOptions);
  }
}
