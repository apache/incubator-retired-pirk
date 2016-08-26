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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.pirk.inputformat.hadoop.InputFormatConst;
import org.apache.pirk.schema.data.DataSchemaLoader;
import org.apache.pirk.schema.query.QuerySchemaLoader;
import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Properties constants and validation for the Responder
 */
public class ResponderProps
{
  private static final Logger logger = LoggerFactory.getLogger(ResponderDriver.class);

  // Required properties
  public static final String PLATFORM = "platform";
  public static final String QUERYINPUT = "pir.queryInput";
  public static final String DATAINPUTFORMAT = "pir.dataInputFormat";
  public static final String OUTPUTFILE = "pir.outputFile";

  // Optional properties
  public static final String INPUTDATA = "pir.inputData";
  public static final String BASEQUERY = "pir.baseQuery";
  public static final String ESRESOURCE = "pir.esResource";
  public static final String ESQUERY = "pir.esQuery";
  public static final String BASEINPUTFORMAT = "pir.baseInputFormat";
  public static final String STOPLISTFILE = "pir.stopListFile";
  public static final String QUERYSCHEMAS = "responder.querySchemas";
  public static final String DATASCHEMAS = "responder.dataSchemas";
  public static final String NUMEXPLOOKUPPARTS = "pir.numExpLookupPartitions";
  public static final String USELOCALCACHE = "pir.useLocalCache";
  public static final String LIMITHITSPERSELECTOR = "pir.limitHitsPerSelector";
  public static final String MAXHITSPERSELECTOR = "pir.maxHitsPerSelector";
  public static final String NUMCOLMULTPARTITIONS = "pir.numColMultPartitions";
  public static final String USEMODEXPJOIN = "pir.useModExpJoin";
  public static final String COLMULTREDUCEBYKEY = "pir.colMultReduceByKey";
  public static final String NUMREDUCETASKS = "pir.numReduceTasks";
  public static final String MAPMEMORY = "mapreduce.map.memory.mb";
  public static final String REDUCEMEMORY = "mapreduce.reduce.memory.mb";
  public static final String MAPJAVAOPTS = "mapreduce.map.java.opts";
  public static final String REDUCEJAVAOPTS = "mapreduce.reduce.java.opts";
  public static final String USEHDFSLOOKUPTABLE = "pir.useHDFSLookupTable";
  public static final String NUMDATAPARTITIONS = "pir.numDataPartitions";
  public static final String ALLOWEMBEDDEDQUERYSCHEMAS = "pir.allowEmbeddedQuerySchemas";
  
  // For Spark Streaming - optional
  public static final String BATCHSECONDS = "pir.sparkstreaming.batchSeconds";
  public static final String WINDOWLENGTH = "pir.sparkstreaming.windowLength";
  public static final String USEQUEUESTREAM = "pir.sparkstreaming.useQueueStream";
  public static final String MAXBATCHES = "pir.sparkstreaming.maxBatches";
  public static final String STOPGRACEFULLY = "spark.streaming.stopGracefullyOnShutdown";

  static final List<String> PROPSLIST = Arrays.asList(PLATFORM, QUERYINPUT, DATAINPUTFORMAT, INPUTDATA, BASEQUERY, ESRESOURCE, ESQUERY, OUTPUTFILE,
      BASEINPUTFORMAT, STOPLISTFILE, NUMREDUCETASKS, USELOCALCACHE, LIMITHITSPERSELECTOR, MAXHITSPERSELECTOR, MAPMEMORY, REDUCEMEMORY, MAPJAVAOPTS,
      REDUCEJAVAOPTS, QUERYSCHEMAS, DATASCHEMAS, NUMEXPLOOKUPPARTS, USEHDFSLOOKUPTABLE, NUMDATAPARTITIONS, NUMCOLMULTPARTITIONS, USEMODEXPJOIN,
      COLMULTREDUCEBYKEY, ALLOWEMBEDDEDQUERYSCHEMAS, BATCHSECONDS, WINDOWLENGTH, USEQUEUESTREAM, MAXBATCHES, STOPGRACEFULLY);

  /**
   * Validates the responder properties
   * 
   */
  static boolean validateResponderProperties()
  {
    boolean valid = true;

    // Parse general required options

    if (!SystemConfiguration.hasProperty(PLATFORM))
    {
      logger.info("Must have the option " + PLATFORM);
      valid = false;
    }

    String platform = SystemConfiguration.getProperty(PLATFORM).toLowerCase();
    if (!platform.equals("mapreduce") && !platform.equals("spark") && !platform.equals("sparkstreaming") && !platform.equals("standalone"))
    {
      logger.info("Unsupported platform: " + platform);
      valid = false;
    }

    if (!SystemConfiguration.hasProperty(QUERYINPUT))
    {
      logger.info("Must have the option " + QUERYINPUT);
      valid = false;
    }

    if (!SystemConfiguration.hasProperty(OUTPUTFILE))
    {
      logger.info("Must have the option " + OUTPUTFILE);
      valid = false;
    }

    if (!SystemConfiguration.hasProperty(DATAINPUTFORMAT))
    {
      logger.info("Must have the option " + DATAINPUTFORMAT);
      valid = false;
    }
    String dataInputFormat = SystemConfiguration.getProperty(DATAINPUTFORMAT).toLowerCase();

    // Parse required properties by dataInputFormat

    if (dataInputFormat.equals(InputFormatConst.BASE_FORMAT))
    {
      if (!SystemConfiguration.hasProperty(BASEINPUTFORMAT))
      {
        logger.info("For base inputformt: Must have the option " + BASEINPUTFORMAT + " if using " + InputFormatConst.BASE_FORMAT);
        valid = false;
      }

      if (!SystemConfiguration.hasProperty(INPUTDATA))
      {
        logger.info("For base inputformt: Must have the option " + INPUTDATA + " if using " + InputFormatConst.BASE_FORMAT);
        valid = false;
      }

      if (!SystemConfiguration.hasProperty(BASEQUERY))
      {
        SystemConfiguration.setProperty("BASEQUERY", "?q=*");
      }
    }
    else if (dataInputFormat.equals(InputFormatConst.ES))
    {
      if (!SystemConfiguration.hasProperty(ESRESOURCE))
      {
        logger.info("For ElasticSearch inputformt: Must have the option " + ESRESOURCE);
        valid = false;
      }

      if (!SystemConfiguration.hasProperty(ESQUERY))
      {
        logger.info("For ElasticSearch inputformat: Must have the option " + ESQUERY);
        valid = false;
      }
    }
    else if (dataInputFormat.equalsIgnoreCase("standalone"))
    {
      if (!SystemConfiguration.hasProperty(INPUTDATA))
      {
        logger.info("Must have the option " + INPUTDATA + " if using " + InputFormatConst.BASE_FORMAT);
        valid = false;
      }
    }
    else
    {
      logger.info("Unsupported inputFormat = " + dataInputFormat);
      valid = false;
    }

    // Parse optional properties

    if (SystemConfiguration.hasProperty(QUERYSCHEMAS))
    {
      SystemConfiguration.appendProperty("query.schemas", SystemConfiguration.getProperty(QUERYSCHEMAS));
    }

    if (SystemConfiguration.hasProperty(DATASCHEMAS))
    {
      SystemConfiguration.appendProperty("data.schemas", SystemConfiguration.getProperty(DATASCHEMAS));
    }

    // Parse optional properties with defaults

    if (!SystemConfiguration.hasProperty(USEHDFSLOOKUPTABLE))
    {
      SystemConfiguration.setProperty(USEHDFSLOOKUPTABLE, "false");
    }

    if (!SystemConfiguration.hasProperty(USEMODEXPJOIN))
    {
      SystemConfiguration.setProperty(USEMODEXPJOIN, "false");
    }

    if (!SystemConfiguration.hasProperty(NUMDATAPARTITIONS))
    {
      SystemConfiguration.setProperty(NUMDATAPARTITIONS, "1000");
    }

    if (!SystemConfiguration.hasProperty(NUMCOLMULTPARTITIONS))
    {
      SystemConfiguration.setProperty(NUMCOLMULTPARTITIONS, "1000");
    }

    if (!SystemConfiguration.hasProperty(COLMULTREDUCEBYKEY))
    {
      SystemConfiguration.setProperty(COLMULTREDUCEBYKEY, "false");
    }

    if (!SystemConfiguration.hasProperty(ALLOWEMBEDDEDQUERYSCHEMAS))
    {
      SystemConfiguration.setProperty(ALLOWEMBEDDEDQUERYSCHEMAS, "false");
    }

    if (!SystemConfiguration.hasProperty(USELOCALCACHE))
    {
      SystemConfiguration.setProperty(USELOCALCACHE, "true");
    }

    if (!SystemConfiguration.hasProperty(BATCHSECONDS))
    {
      SystemConfiguration.setProperty(BATCHSECONDS, "30");
    }

    if (!SystemConfiguration.hasProperty(WINDOWLENGTH))
    {
      SystemConfiguration.setProperty(WINDOWLENGTH, "30");
    }

    if (!SystemConfiguration.hasProperty(USEQUEUESTREAM))
    {
      SystemConfiguration.setProperty(USEQUEUESTREAM, "false");
    }

    if (!SystemConfiguration.hasProperty(MAXBATCHES))
    {
      SystemConfiguration.setProperty(MAXBATCHES, "-1");
    }

    if (!SystemConfiguration.hasProperty(STOPGRACEFULLY))
    {
      SystemConfiguration.setProperty(STOPGRACEFULLY, "false");
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
