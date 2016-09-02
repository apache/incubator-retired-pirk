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
package org.apache.pirk.responder.wideskies;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for parsing the command line options for the ResponderDriver
 */
public class ResponderCLI
{
  private static final Logger logger = LoggerFactory.getLogger(ResponderCLI.class);

  private Options cliOptions = null;
  private CommandLine commandLine = null;

  private static final String LOCALPROPFILE = "local.responder.properties";

  /**
   * Create and parse allowable options
   *
   */
  public ResponderCLI(String[] args)
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
    boolean valid;

    // If we have a local.querier.properties file specified, load it
    if (hasOption(LOCALPROPFILE))
    {
      SystemConfiguration.loadPropsFromFile(new File(getOptionValue(LOCALPROPFILE)));
    }
    else
    {
      // Pull options, set as properties
      for (String prop : ResponderProps.PROPSLIST)
      {
        if (hasOption(prop))
        {
          SystemConfiguration.setProperty(prop, getOptionValue(prop));
        }
      }
    }

    // Validate properties
    valid = ResponderProps.validateResponderProperties();

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
    Option optionLocalPropFile = new Option("localPropFile", LOCALPROPFILE, true, "Optional local properties file");
    optionLocalPropFile.setRequired(false);
    optionLocalPropFile.setArgName(LOCALPROPFILE);
    optionLocalPropFile.setType(String.class);
    options.addOption(optionLocalPropFile);

    // platform
    Option optionPlatform = new Option("p", ResponderProps.PLATFORM, true,
        "required -- 'mapreduce', 'spark', or 'standalone' : Processing platform technology for the responder");
    optionPlatform.setRequired(false);
    optionPlatform.setArgName(ResponderProps.PLATFORM);
    optionPlatform.setType(String.class);
    options.addOption(optionPlatform);

    // queryInput
    Option optionQueryInput = new Option("q", ResponderProps.QUERYINPUT, true, "required -- Fully qualified dir in hdfs of Query files");
    optionQueryInput.setRequired(false);
    optionQueryInput.setArgName(ResponderProps.QUERYINPUT);
    optionQueryInput.setType(String.class);
    options.addOption(optionQueryInput);

    // dataInputFormat
    Option optionDataInputFormat = new Option("d", ResponderProps.DATAINPUTFORMAT, true,
        "required -- 'base', 'elasticsearch', or 'standalone' : Specify the input format");
    optionDataInputFormat.setRequired(false);
    optionDataInputFormat.setArgName(ResponderProps.DATAINPUTFORMAT);
    optionDataInputFormat.setType(String.class);
    options.addOption(optionDataInputFormat);

    // inputData
    Option optionInputData = new Option("i", ResponderProps.INPUTDATA, true,
        "required -- Fully qualified name of input file/directory in hdfs; used if inputFormat = 'base'");
    optionInputData.setRequired(false);
    optionInputData.setArgName(ResponderProps.INPUTDATA);
    optionInputData.setType(String.class);
    options.addOption(optionInputData);

    // baseInputFormat
    Option optionBaseInputFormat = new Option("bif", ResponderProps.BASEINPUTFORMAT, true,
        "required if baseInputFormat = 'base' -- Full class name of the InputFormat to use when reading in the data - must extend BaseInputFormat");
    optionBaseInputFormat.setRequired(false);
    optionBaseInputFormat.setArgName(ResponderProps.BASEINPUTFORMAT);
    optionBaseInputFormat.setType(String.class);
    options.addOption(optionBaseInputFormat);

    // baseQuery
    Option optionBaseQuery = new Option("j", ResponderProps.BASEQUERY, true,
        "optional -- ElasticSearch-like query if using 'base' input format - used to filter records in the RecordReader");
    optionBaseQuery.setRequired(false);
    optionBaseQuery.setArgName(ResponderProps.BASEQUERY);
    optionBaseQuery.setType(String.class);
    options.addOption(optionBaseQuery);

    // esResource
    Option optionEsResource = new Option("er", ResponderProps.ESRESOURCE, true,
        "required if baseInputFormat = 'elasticsearch' -- Requires the format <index>/<type> : Elasticsearch resource where data is read and written to");
    optionEsResource.setRequired(false);
    optionEsResource.setArgName(ResponderProps.ESRESOURCE);
    optionEsResource.setType(String.class);
    options.addOption(optionEsResource);

    // esQuery
    Option optionEsQuery = new Option("eq", ResponderProps.ESQUERY, true,
        "required if baseInputFormat = 'elasticsearch' -- ElasticSearch query if using 'elasticsearch' input format");
    optionEsQuery.setRequired(false);
    optionEsQuery.setArgName(ResponderProps.ESQUERY);
    optionEsQuery.setType(String.class);
    options.addOption(optionEsQuery);

    // esNodes
    Option optionEsNodes = new Option("en", ResponderProps.ESNODES, true, "required if baseInputFormat = 'elasticsearch' -- ElasticSearch node in the cluster");
    optionEsNodes.setRequired(false);
    optionEsNodes.setArgName(ResponderProps.ESNODES);
    optionEsNodes.setType(String.class);
    options.addOption(optionEsNodes);

    // esPort
    Option optionEsPort = new Option("ep", ResponderProps.ESPORT, true, "required if baseInputFormat = 'elasticsearch' -- ElasticSearch cluster port");
    optionEsPort.setRequired(false);
    optionEsPort.setArgName(ResponderProps.ESPORT);
    optionEsPort.setType(String.class);
    options.addOption(optionEsPort);

    // outputFile
    Option optionOutputFile = new Option("o", ResponderProps.OUTPUTFILE, true, "required -- Fully qualified name of output file in hdfs");
    optionOutputFile.setRequired(false);
    optionOutputFile.setArgName(ResponderProps.OUTPUTFILE);
    optionOutputFile.setType(String.class);
    options.addOption(optionOutputFile);

    // stopListFile
    Option optionStopListFile = new Option("sf", ResponderProps.STOPLISTFILE, true,
        "optional (unless using StopListFilter) -- Fully qualified file in hdfs containing stoplist terms; used by the StopListFilter");
    optionStopListFile.setRequired(false);
    optionStopListFile.setArgName(ResponderProps.STOPLISTFILE);
    optionStopListFile.setType(String.class);
    options.addOption(optionStopListFile);

    // numReduceTasks
    Option optionNumReduceTasks = new Option("nr", ResponderProps.NUMREDUCETASKS, true, "optional -- Number of reduce tasks");
    optionNumReduceTasks.setRequired(false);
    optionNumReduceTasks.setArgName(ResponderProps.NUMREDUCETASKS);
    optionNumReduceTasks.setType(String.class);
    options.addOption(optionNumReduceTasks);

    // useLocalCache
    Option optionUseLocalCache = new Option("ulc", ResponderProps.USELOCALCACHE, true,
        "optional -- 'true' or 'false : Whether or not to use the local cache for modular exponentiation; Default is 'true'");
    optionUseLocalCache.setRequired(false);
    optionUseLocalCache.setArgName(ResponderProps.USELOCALCACHE);
    optionUseLocalCache.setType(String.class);
    options.addOption(optionUseLocalCache);

    // limitHitsPerSelector
    Option optionLimitHitsPerSelector = new Option("lh", ResponderProps.LIMITHITSPERSELECTOR, true,
        "optional -- 'true' or 'false : Whether or not to limit the number of hits per selector; Default is 'true'");
    optionLimitHitsPerSelector.setRequired(false);
    optionLimitHitsPerSelector.setArgName(ResponderProps.LIMITHITSPERSELECTOR);
    optionLimitHitsPerSelector.setType(String.class);
    options.addOption(optionLimitHitsPerSelector);

    // maxHitsPerSelector
    Option optionMaxHitsPerSelector = new Option("mh", ResponderProps.MAXHITSPERSELECTOR, true, "optional -- Max number of hits encrypted per selector");
    optionMaxHitsPerSelector.setRequired(false);
    optionMaxHitsPerSelector.setArgName(ResponderProps.MAXHITSPERSELECTOR);
    optionMaxHitsPerSelector.setType(String.class);
    options.addOption(optionMaxHitsPerSelector);

    // mapreduce.map.memory.mb
    Option optionMapMemory = new Option("mm", ResponderProps.MAPMEMORY, true, "optional -- Amount of memory (in MB) to allocate per map task; Default is 3000");
    optionMapMemory.setRequired(false);
    optionMapMemory.setArgName(ResponderProps.MAPMEMORY);
    optionMapMemory.setType(String.class);
    options.addOption(optionMapMemory);

    // mapreduce.reduce.memory.mb
    Option optionReduceMemory = new Option("rm", ResponderProps.REDUCEMEMORY, true,
        "optional -- Amount of memory (in MB) to allocate per reduce task; Default is 3000");
    optionReduceMemory.setRequired(false);
    optionReduceMemory.setArgName(ResponderProps.REDUCEMEMORY);
    optionReduceMemory.setType(String.class);
    options.addOption(optionReduceMemory);

    // mapreduce.map.java.opts
    Option optionMapOpts = new Option("mjo", ResponderProps.MAPJAVAOPTS, true,
        "optional -- Amount of heap (in MB) to allocate per map task; Default is -Xmx2800m");
    optionMapOpts.setRequired(false);
    optionMapOpts.setArgName(ResponderProps.MAPJAVAOPTS);
    optionMapOpts.setType(String.class);
    options.addOption(optionMapOpts);

    // mapreduce.reduce.java.opts
    Option optionReduceOpts = new Option("rjo", ResponderProps.REDUCEJAVAOPTS, true,
        "optional -- Amount of heap (in MB) to allocate per reduce task; Default is -Xmx2800m");
    optionReduceOpts.setRequired(false);
    optionReduceOpts.setArgName(ResponderProps.REDUCEJAVAOPTS);
    optionReduceOpts.setType(String.class);
    options.addOption(optionReduceOpts);

    // data.schemas
    Option optionDataSchemas = new Option("ds", ResponderProps.DATASCHEMAS, true, "required -- Comma separated list of data schema file names");
    optionDataSchemas.setRequired(false);
    optionDataSchemas.setArgName(ResponderProps.DATASCHEMAS);
    optionDataSchemas.setType(String.class);
    options.addOption(optionDataSchemas);

    // query.schemas
    Option optionQuerySchemas = new Option("qs", ResponderProps.QUERYSCHEMAS, true, "required -- Comma separated list of query schema file names");
    optionQuerySchemas.setRequired(false);
    optionQuerySchemas.setArgName(ResponderProps.QUERYSCHEMAS);
    optionQuerySchemas.setType(String.class);
    options.addOption(optionQuerySchemas);

    // pir.numExpLookupPartitions
    Option optionExpParts = new Option("expParts", ResponderProps.NUMEXPLOOKUPPARTS, true, "optional -- Number of partitions for the exp lookup table");
    optionExpParts.setRequired(false);
    optionExpParts.setArgName(ResponderProps.NUMEXPLOOKUPPARTS);
    optionExpParts.setType(String.class);
    options.addOption(optionExpParts);

    // pir.numExpLookupPartitions
    Option optionHdfsExp = new Option("hdfsExp", ResponderProps.USEHDFSLOOKUPTABLE, true,
        "optional -- 'true' or 'false' - Whether or not to generate and use the hdfs lookup table" + " for modular exponentiation");
    optionHdfsExp.setRequired(false);
    optionHdfsExp.setArgName(ResponderProps.USEHDFSLOOKUPTABLE);
    optionHdfsExp.setType(String.class);
    options.addOption(optionHdfsExp);

    // numDataPartitions
    Option optionDataParts = new Option("dataParts", ResponderProps.NUMDATAPARTITIONS, true, "optional -- Number of partitions for the input data");
    optionDataParts.setRequired(false);
    optionDataParts.setArgName(ResponderProps.NUMDATAPARTITIONS);
    optionDataParts.setType(String.class);
    options.addOption(optionDataParts);

    // useModExpJoin
    Option optionModExpJoin = new Option("useModExpJoin", ResponderProps.USEMODEXPJOIN, true,
        "optional -- 'true' or 'false' -- Spark only -- Whether or not to "
            + "pre-compute the modular exponentiation table and join it to the data partitions when performing the encrypted row calculations");
    optionModExpJoin.setRequired(false);
    optionModExpJoin.setArgName(ResponderProps.USEMODEXPJOIN);
    optionModExpJoin.setType(String.class);
    options.addOption(optionModExpJoin);

    // numColMultPartitions
    Option optionNumColMultPartitions = new Option("numColMultParts", ResponderProps.NUMCOLMULTPARTITIONS, true,
        "optional, Spark only -- Number of partitions to " + "use when performing column multiplication");
    optionNumColMultPartitions.setRequired(false);
    optionNumColMultPartitions.setArgName(ResponderProps.NUMCOLMULTPARTITIONS);
    optionNumColMultPartitions.setType(String.class);
    options.addOption(optionNumColMultPartitions);

    // colMultReduceByKey
    Option optionColMultReduceByKey = new Option("colMultRBK", ResponderProps.COLMULTREDUCEBYKEY, true, "optional -- 'true' or 'false' -- Spark only -- "
        + "If true, uses reduceByKey in performing column multiplication; if false, uses groupByKey -> reduce");
    optionColMultReduceByKey.setRequired(false);
    optionColMultReduceByKey.setArgName(ResponderProps.COLMULTREDUCEBYKEY);
    optionColMultReduceByKey.setType(String.class);
    options.addOption(optionColMultReduceByKey);

    // allowEmbeddedQS
    Option optionAllowEmbeddedQS = new Option("allowEmbeddedQS", ResponderProps.ALLOWEMBEDDEDQUERYSCHEMAS, true,
        "optional -- 'true' or 'false'  (defaults to 'false') -- " + "If true, allows embedded QuerySchemas for a query.");
    optionAllowEmbeddedQS.setRequired(false);
    optionAllowEmbeddedQS.setArgName(ResponderProps.ALLOWEMBEDDEDQUERYSCHEMAS);
    optionAllowEmbeddedQS.setType(String.class);
    options.addOption(optionAllowEmbeddedQS);

    // batchSeconds - spark streaming
    Option optionBatchSeconds = new Option("batchSeconds", ResponderProps.BATCHSECONDS, true,
        "optional -- Number of seconds per batch in Spark Streaming; defaults to 30");
    optionBatchSeconds.setRequired(false);
    optionBatchSeconds.setArgName(ResponderProps.BATCHSECONDS);
    optionBatchSeconds.setType(String.class);
    options.addOption(optionBatchSeconds);

    // windowLength - spark streaming
    Option optionWindowLength = new Option("windowLength", ResponderProps.WINDOWLENGTH, true,
        "optional -- Number of seconds per window in Spark Streaming; defaults to 60");
    optionWindowLength.setRequired(false);
    optionWindowLength.setArgName(ResponderProps.WINDOWLENGTH);
    optionWindowLength.setType(String.class);
    options.addOption(optionWindowLength);

    // maxBatches - spark streaming
    Option optionMaxBatches = new Option("maxBatches", ResponderProps.MAXBATCHES, true,
        "optional -- Max batches to process in Spark Streaming; defaults to -1 - unlimited");
    optionMaxBatches.setRequired(false);
    optionMaxBatches.setArgName(ResponderProps.MAXBATCHES);
    optionMaxBatches.setType(String.class);
    options.addOption(optionMaxBatches);

    // stopGracefully - spark streaming
    Option optionStopGracefully = new Option("stopGracefully", ResponderProps.STOPGRACEFULLY, true,
        "optional -- Whether or not to stop gracefully in Spark Streaming; defaults to false");
    optionStopGracefully.setRequired(false);
    optionStopGracefully.setArgName(ResponderProps.STOPGRACEFULLY);
    optionStopGracefully.setType(String.class);
    options.addOption(optionStopGracefully);

    // useQueueStream - spark streaming
    Option optionUseQueueStream = new Option("queueStream", ResponderProps.USEQUEUESTREAM, true,
        "optional -- Whether or not to use a queue stream in Spark Streaming; defaults to false");
    optionUseQueueStream.setRequired(false);
    optionUseQueueStream.setArgName(ResponderProps.USEQUEUESTREAM);
    optionUseQueueStream.setType(String.class);
    options.addOption(optionUseQueueStream);

    return options;
  }

  /**
   * Prints out the help message
   */
  private void printHelp()
  {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(140);
    formatter.printHelp("ResponderDriver", cliOptions);
  }
}
