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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.pirk.inputformat.hadoop.InputFormatConst;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.query.LoadQuerySchemas;
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

  // Required args
  public static String PLATFORM = "platform";
  public static String QUERYINPUT = "queryInput";
  public static String DATAINPUTFORMAT = "dataInputFormat";
  public static String INPUTDATA = "inputData";
  public static String BASEQUERY = "baseQuery";
  public static String ESRESOURCE = "esResource";
  public static String ESQUERY = "esQuery";
  public static String OUTPUTFILE = "outputFile";

  // Optional args
  public static String BASEINPUTFORMAT = "baseInputFormat";
  public static String STOPLISTFILE = "stopListFile";
  private static String NUMREDUCETASKS = "numReduceTasks";
  public static String USELOCALCACHE = "useLocalCache";
  public static String LIMITHITSPERSELECTOR = "limitHitsPerSelector";
  public static String MAXHITSPERSELECTOR = "maxHitsPerSelector";
  private static String MAPMEMORY = "mapreduceMapMemoryMb";
  private static String REDUCEMEMORY = "mapreduceReduceMemoryMb";
  private static String MAPJAVAOPTS = "mapreduceMapJavaOpts";
  private static String REDUCEJAVAOPTS = "mapreduceReduceJavaOpts";
  public static String QUERYSCHEMAS = "querySchemas";
  public static String DATASCHEMAS = "dataSchemas";
  public static String NUMEXPLOOKUPPARTS = "numExpLookupPartitions";
  private static String USEHDFSLOOKUPTABLE = "useHDFSLookupTable";
  private static String NUMDATAPARTITIONS = "numDataPartitions";
  public static String NUMCOLMULTPARTITIONS = "numColMultPartitions";
  public static String USEMODEXPJOIN = "useModExpJoin";
  public static String COLMULTREDUCEBYKEY = "colMultReduceByKey";
  public static String ALLOWEMBEDDEDQUERYSCHEMAS = "allowAdHocQuerySchemas";

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
    boolean valid = true;

    // Parse general required options
    if (!hasOption(PLATFORM))
    {
      logger.info("Must have the option " + PLATFORM);
      return false;
    }
    String platform = getOptionValue(PLATFORM).toLowerCase();
    if (!platform.equals("mapreduce") && !platform.equals("spark") && !platform.equals("standalone"))
    {
      logger.info("Unsupported platform: " + platform);
      return false;
    }
    SystemConfiguration.setProperty("platform", getOptionValue(PLATFORM));

    if (!hasOption(QUERYINPUT))
    {
      logger.info("Must have the option " + QUERYINPUT);
      return false;
    }
    SystemConfiguration.setProperty("pir.queryInput", getOptionValue(QUERYINPUT));

    if (!hasOption(OUTPUTFILE))
    {
      logger.info("Must have the option " + OUTPUTFILE);
      return false;
    }
    SystemConfiguration.setProperty("pir.outputFile", getOptionValue(OUTPUTFILE));

    if (!hasOption(QUERYSCHEMAS))
    {
      logger.info("Must have the option " + QUERYSCHEMAS);
      return false;
    }
    SystemConfiguration.setProperty("query.schemas", getOptionValue(QUERYSCHEMAS));

    if (!hasOption(DATASCHEMAS))
    {
      logger.info("Must have the option " + DATASCHEMAS);
      return false;
    }
    SystemConfiguration.setProperty("data.schemas", getOptionValue(DATASCHEMAS));

    if (!hasOption(DATAINPUTFORMAT))
    {
      logger.info("Must have the option " + DATAINPUTFORMAT);
      return false;
    }
    String dataInputFormat = getOptionValue(DATAINPUTFORMAT).toLowerCase();
    SystemConfiguration.setProperty("pir.dataInputFormat", dataInputFormat);

    // Parse required options by dataInputFormat
    if (dataInputFormat.equals(InputFormatConst.BASE_FORMAT))
    {
      if (!hasOption(BASEINPUTFORMAT))
      {
        logger.info("Must have the option " + BASEINPUTFORMAT + " if using " + InputFormatConst.BASE_FORMAT);
        return false;
      }
      SystemConfiguration.setProperty("pir.baseInputFormat", getOptionValue(BASEINPUTFORMAT));

      if (!hasOption(INPUTDATA))
      {
        logger.info("Must have the option " + INPUTDATA + " if using " + InputFormatConst.BASE_FORMAT);
        return false;
      }
      SystemConfiguration.setProperty("pir.inputData", getOptionValue(INPUTDATA));

      if (hasOption(BASEQUERY))
      {
        SystemConfiguration.setProperty("pir.baseQuery", getOptionValue(BASEQUERY));
      }
      else
      {
        SystemConfiguration.setProperty("pir.baseQuery", "?q=*");
      }
    }
    else if (dataInputFormat.equals(InputFormatConst.ES))
    {
      if (!hasOption(ESRESOURCE))
      {
        logger.info("Must have the option " + ESRESOURCE);
        return false;
      }
      SystemConfiguration.setProperty("pir.esResource", getOptionValue(ESRESOURCE));

      if (!hasOption(ESQUERY))
      {
        logger.info("Must have the option " + ESQUERY);
        return false;
      }
      SystemConfiguration.setProperty("pir.esQuery", getOptionValue(ESQUERY));
    }
    else if (dataInputFormat.equalsIgnoreCase("standalone"))
    {
      if (!hasOption(INPUTDATA))
      {
        logger.info("Must have the option " + INPUTDATA + " if using " + InputFormatConst.BASE_FORMAT);
        return false;
      }
      SystemConfiguration.setProperty("pir.inputData", getOptionValue(INPUTDATA));
    }
    else
    {
      logger.info("Unsupported inputFormat = " + dataInputFormat);
      return false;
    }

    // Parse optional args
    if (hasOption(STOPLISTFILE))
    {
      SystemConfiguration.setProperty("pir.stopListFile", getOptionValue(STOPLISTFILE));
    }

    if (hasOption(NUMREDUCETASKS))
    {
      SystemConfiguration.setProperty("pir.numReduceTasks", getOptionValue(NUMREDUCETASKS));
    }

    if (hasOption(USELOCALCACHE))
    {
      SystemConfiguration.setProperty("pir.useLocalCache", getOptionValue(USELOCALCACHE));
    }

    if (hasOption(LIMITHITSPERSELECTOR))
    {
      SystemConfiguration.setProperty("pir.limitHitsPerSelector", getOptionValue(LIMITHITSPERSELECTOR));
    }

    if (hasOption(MAXHITSPERSELECTOR))
    {
      SystemConfiguration.setProperty("pir.maxHitsPerSelector", getOptionValue(MAXHITSPERSELECTOR));
    }

    if (hasOption(MAPMEMORY))
    {
      SystemConfiguration.setProperty("mapreduce.map.memory.mb", getOptionValue(MAPMEMORY));
    }

    if (hasOption(REDUCEMEMORY))
    {
      SystemConfiguration.setProperty("mapreduce.reduce.memory.mb", getOptionValue(REDUCEMEMORY));
    }

    if (hasOption(MAPJAVAOPTS))
    {
      SystemConfiguration.setProperty("mapreduce.map.java.opts", getOptionValue(MAPJAVAOPTS));
    }

    if (hasOption(REDUCEJAVAOPTS))
    {
      SystemConfiguration.setProperty("mapreduce.reduce.java.opts", getOptionValue(REDUCEJAVAOPTS));
    }

    if (hasOption(NUMEXPLOOKUPPARTS))
    {
      SystemConfiguration.setProperty("pir.numExpLookupPartitions", getOptionValue(NUMEXPLOOKUPPARTS));
    }

    if (hasOption(USEHDFSLOOKUPTABLE))
    {
      SystemConfiguration.setProperty("pir.useHDFSLookupTable", getOptionValue(USEHDFSLOOKUPTABLE));
    }
    else
    {
      SystemConfiguration.setProperty("pir.useHDFSLookupTable", "false");
    }

    if (hasOption(USEMODEXPJOIN))
    {
      SystemConfiguration.setProperty("pir.useModExpJoin", getOptionValue(USEMODEXPJOIN));
    }
    else
    {
      SystemConfiguration.setProperty("pir.useModExpJoin", "false");
    }

    if (hasOption(NUMDATAPARTITIONS))
    {
      SystemConfiguration.setProperty("pir.numDataPartitions", getOptionValue(NUMDATAPARTITIONS));
    }
    else
    {
      SystemConfiguration.setProperty("pir.numDataPartitions", "1000");
    }

    if (hasOption(NUMCOLMULTPARTITIONS))
    {
      SystemConfiguration.setProperty("pir.numColMultPartitions", getOptionValue(NUMCOLMULTPARTITIONS));
    }
    else
    {
      SystemConfiguration.setProperty("pir.numColMultPartitions", "1000");
    }

    if (hasOption(COLMULTREDUCEBYKEY))
    {
      SystemConfiguration.setProperty("pir.colMultReduceByKey", getOptionValue(COLMULTREDUCEBYKEY));
    }
    else
    {
      SystemConfiguration.setProperty("pir.colMultReduceByKey", "false");
    }

    if (hasOption(ALLOWEMBEDDEDQUERYSCHEMAS))
    {
      SystemConfiguration.setProperty("pir.allowEmbeddedQuerySchemas", getOptionValue(ALLOWEMBEDDEDQUERYSCHEMAS));
    }
    else
    {
      SystemConfiguration.setProperty("pir.allowEmbeddedQuerySchemas", "false");
    }

    // Load the new local query and data schemas
    try
    {
      DataSchemaLoader.initialize();
      LoadQuerySchemas.initialize();

    } catch (Exception e)
    {
      e.printStackTrace();
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

    // platform
    Option optionPlatform = new Option("p", PLATFORM, true,
        "required -- 'mapreduce', 'spark', or 'standalone' : Processing platform technology for the responder");
    optionPlatform.setRequired(false);
    optionPlatform.setArgName(PLATFORM);
    optionPlatform.setType(String.class);
    options.addOption(optionPlatform);

    // queryInput
    Option optionQueryInput = new Option("q", QUERYINPUT, true, "required -- Fully qualified dir in hdfs of Query files");
    optionQueryInput.setRequired(false);
    optionQueryInput.setArgName(QUERYINPUT);
    optionQueryInput.setType(String.class);
    options.addOption(optionQueryInput);

    // dataInputFormat
    Option optionDataInputFormat = new Option("d", DATAINPUTFORMAT, true, "required -- 'base', 'elasticsearch', or 'standalone' : Specify the input format");
    optionDataInputFormat.setRequired(false);
    optionDataInputFormat.setArgName(DATAINPUTFORMAT);
    optionDataInputFormat.setType(String.class);
    options.addOption(optionDataInputFormat);

    // inputData
    Option optionInputData = new Option("i", INPUTDATA, true, "required -- Fully qualified name of input file/directory in hdfs; used if inputFormat = 'base'");
    optionInputData.setRequired(false);
    optionInputData.setArgName(INPUTDATA);
    optionInputData.setType(String.class);
    options.addOption(optionInputData);

    // baseInputFormat
    Option optionBaseInputFormat = new Option("bif", BASEINPUTFORMAT, true,
        "required if baseInputFormat = 'base' -- Full class name of the InputFormat to use when reading in the data - must extend BaseInputFormat");
    optionBaseInputFormat.setRequired(false);
    optionBaseInputFormat.setArgName(BASEINPUTFORMAT);
    optionBaseInputFormat.setType(String.class);
    options.addOption(optionBaseInputFormat);

    // baseQuery
    Option optionBaseQuery = new Option("j", BASEQUERY, true,
        "optional -- ElasticSearch-like query if using 'base' input format - used to filter records in the RecordReader");
    optionBaseQuery.setRequired(false);
    optionBaseQuery.setArgName(BASEQUERY);
    optionBaseQuery.setType(String.class);
    options.addOption(optionBaseQuery);

    // esResource
    Option optionEsResource = new Option("er", ESRESOURCE, true,
        "required if baseInputFormat = 'elasticsearch' -- Requires the format <index>/<type> : Elasticsearch resource where data is read and written to");
    optionEsResource.setRequired(false);
    optionEsResource.setArgName(ESRESOURCE);
    optionEsResource.setType(String.class);
    options.addOption(optionEsResource);

    // esQuery
    Option optionEsQuery = new Option("eq", ESQUERY, true,
        "required if baseInputFormat = 'elasticsearch' -- ElasticSearch query if using 'elasticsearch' input format");
    optionEsQuery.setRequired(false);
    optionEsQuery.setArgName(ESQUERY);
    optionEsQuery.setType(String.class);
    options.addOption(optionEsQuery);

    // outputFile
    Option optionOutputFile = new Option("o", OUTPUTFILE, true, "required -- Fully qualified name of output file in hdfs");
    optionOutputFile.setRequired(false);
    optionOutputFile.setArgName(OUTPUTFILE);
    optionOutputFile.setType(String.class);
    options.addOption(optionOutputFile);

    // stopListFile
    Option optionStopListFile = new Option("sf", STOPLISTFILE, true,
        "optional (unless using StopListFilter) -- Fully qualified file in hdfs containing stoplist terms; used by the StopListFilter");
    optionStopListFile.setRequired(false);
    optionStopListFile.setArgName(STOPLISTFILE);
    optionStopListFile.setType(String.class);
    options.addOption(optionStopListFile);

    // numReduceTasks
    Option optionNumReduceTasks = new Option("nr", NUMREDUCETASKS, true, "optional -- Number of reduce tasks");
    optionNumReduceTasks.setRequired(false);
    optionNumReduceTasks.setArgName(NUMREDUCETASKS);
    optionNumReduceTasks.setType(String.class);
    options.addOption(optionNumReduceTasks);

    // useLocalCache
    Option optionUseLocalCache = new Option("ulc", USELOCALCACHE, true,
        "optional -- 'true' or 'false : Whether or not to use the local cache for modular exponentiation; Default is 'true'");
    optionUseLocalCache.setRequired(false);
    optionUseLocalCache.setArgName(USELOCALCACHE);
    optionUseLocalCache.setType(String.class);
    options.addOption(optionUseLocalCache);

    // limitHitsPerSelector
    Option optionLimitHitsPerSelector = new Option("lh", LIMITHITSPERSELECTOR, true,
        "optional -- 'true' or 'false : Whether or not to limit the number of hits per selector; Default is 'true'");
    optionLimitHitsPerSelector.setRequired(false);
    optionLimitHitsPerSelector.setArgName(LIMITHITSPERSELECTOR);
    optionLimitHitsPerSelector.setType(String.class);
    options.addOption(optionLimitHitsPerSelector);

    // maxHitsPerSelector
    Option optionMaxHitsPerSelector = new Option("mh", MAXHITSPERSELECTOR, true, "optional -- Max number of hits encrypted per selector");
    optionMaxHitsPerSelector.setRequired(false);
    optionMaxHitsPerSelector.setArgName(MAXHITSPERSELECTOR);
    optionMaxHitsPerSelector.setType(String.class);
    options.addOption(optionMaxHitsPerSelector);

    // mapreduce.map.memory.mb
    Option optionMapMemory = new Option("mm", MAPMEMORY, true, "optional -- Amount of memory (in MB) to allocate per map task; Default is 3000");
    optionMapMemory.setRequired(false);
    optionMapMemory.setArgName(MAPMEMORY);
    optionMapMemory.setType(String.class);
    options.addOption(optionMapMemory);

    // mapreduce.reduce.memory.mb
    Option optionReduceMemory = new Option("rm", REDUCEMEMORY, true, "optional -- Amount of memory (in MB) to allocate per reduce task; Default is 3000");
    optionReduceMemory.setRequired(false);
    optionReduceMemory.setArgName(REDUCEMEMORY);
    optionReduceMemory.setType(String.class);
    options.addOption(optionReduceMemory);

    // mapreduce.map.java.opts
    Option optionMapOpts = new Option("mjo", MAPJAVAOPTS, true, "optional -- Amount of heap (in MB) to allocate per map task; Default is -Xmx2800m");
    optionMapOpts.setRequired(false);
    optionMapOpts.setArgName(MAPJAVAOPTS);
    optionMapOpts.setType(String.class);
    options.addOption(optionMapOpts);

    // mapreduce.reduce.java.opts
    Option optionReduceOpts = new Option("rjo", REDUCEJAVAOPTS, true, "optional -- Amount of heap (in MB) to allocate per reduce task; Default is -Xmx2800m");
    optionReduceOpts.setRequired(false);
    optionReduceOpts.setArgName(REDUCEJAVAOPTS);
    optionReduceOpts.setType(String.class);
    options.addOption(optionReduceOpts);

    // data.schemas
    Option optionDataSchemas = new Option("ds", DATASCHEMAS, true, "required -- Comma separated list of data schema file names");
    optionDataSchemas.setRequired(false);
    optionDataSchemas.setArgName(DATASCHEMAS);
    optionDataSchemas.setType(String.class);
    options.addOption(optionDataSchemas);

    // query.schemas
    Option optionQuerySchemas = new Option("qs", QUERYSCHEMAS, true, "required -- Comma separated list of query schema file names");
    optionQuerySchemas.setRequired(false);
    optionQuerySchemas.setArgName(QUERYSCHEMAS);
    optionQuerySchemas.setType(String.class);
    options.addOption(optionQuerySchemas);

    // pir.numExpLookupPartitions
    Option optionExpParts = new Option("expParts", NUMEXPLOOKUPPARTS, true, "optional -- Number of partitions for the exp lookup table");
    optionExpParts.setRequired(false);
    optionExpParts.setArgName(NUMEXPLOOKUPPARTS);
    optionExpParts.setType(String.class);
    options.addOption(optionExpParts);

    // pir.numExpLookupPartitions
    Option optionHdfsExp = new Option("hdfsExp", USEHDFSLOOKUPTABLE, true,
        "optional -- 'true' or 'false' - Whether or not to generate and use the hdfs lookup table" + " for modular exponentiation");
    optionHdfsExp.setRequired(false);
    optionHdfsExp.setArgName(USEHDFSLOOKUPTABLE);
    optionHdfsExp.setType(String.class);
    options.addOption(optionHdfsExp);

    // numDataPartitions
    Option optionDataParts = new Option("dataParts", NUMDATAPARTITIONS, true, "optional -- Number of partitions for the input data");
    optionDataParts.setRequired(false);
    optionDataParts.setArgName(NUMDATAPARTITIONS);
    optionDataParts.setType(String.class);
    options.addOption(optionDataParts);

    // useModExpJoin
    Option optionModExpJoin = new Option("useModExpJoin", USEMODEXPJOIN, true, "optional -- 'true' or 'false' -- Spark only -- Whether or not to "
        + "pre-compute the modular exponentiation table and join it to the data partitions when performing the encrypted row calculations");
    optionModExpJoin.setRequired(false);
    optionModExpJoin.setArgName(USEMODEXPJOIN);
    optionModExpJoin.setType(String.class);
    options.addOption(optionModExpJoin);

    // numColMultPartitions
    Option optionNumColMultPartitions = new Option("numColMultParts", NUMCOLMULTPARTITIONS, true, "optional, Spark only -- Number of partitions to "
        + "use when performing column multiplication");
    optionNumColMultPartitions.setRequired(false);
    optionNumColMultPartitions.setArgName(NUMCOLMULTPARTITIONS);
    optionNumColMultPartitions.setType(String.class);
    options.addOption(optionNumColMultPartitions);

    // colMultReduceByKey
    Option optionColMultReduceByKey = new Option("colMultRBK", COLMULTREDUCEBYKEY, true, "optional -- 'true' or 'false' -- Spark only -- "
        + "If true, uses reduceByKey in performing column multiplication; if false, uses groupByKey -> reduce");
    optionColMultReduceByKey.setRequired(false);
    optionColMultReduceByKey.setArgName(COLMULTREDUCEBYKEY);
    optionColMultReduceByKey.setType(String.class);
    options.addOption(optionColMultReduceByKey);

    // colMultReduceByKey
    Option optionAllowEmbeddedQS = new Option("allowEmbeddedQS", ALLOWEMBEDDEDQUERYSCHEMAS, true, "optional -- 'true' or 'false'  (defaults to 'false') -- "
        + "If true, allows embedded QuerySchemas for a query.");
    optionAllowEmbeddedQS.setRequired(false);
    optionAllowEmbeddedQS.setArgName(ALLOWEMBEDDEDQUERYSCHEMAS);
    optionAllowEmbeddedQS.setType(String.class);
    options.addOption(optionAllowEmbeddedQS);

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
