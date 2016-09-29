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
package org.apache.pirk.test.distributed;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container for Apache's Command Line Interface that contains custom functionality for the MapReduce functional tests.
 */
public class DistributedTestCLI
{
  private static final Logger logger = LoggerFactory.getLogger(DistributedTestCLI.class);

  private CommandLine commandLine = null;
  private Options cliOptions = null;

  /**
   * Create and parse allowable options
   * 
   * @param args
   *          - arguments fed into the main method
   */
  public DistributedTestCLI(String[] args)
  {
    // create the command line options
    cliOptions = createOptions();

    try
    {
      // parse the command line options
      CommandLineParser parser = new GnuParser();
      commandLine = parser.parse(cliOptions, args, true);

      // if help option is selected, just print help text and exit
      if (hasOption("h"))
      {
        printHelp();
        System.exit(1);
      }

      // The full path of the jar file must be set
      if (!hasOption("j"))
      {
        logger.info("The full path of the jar file must be set with -j");
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
   * Determine if the argument was provided, which determines if a test should or should not be run
   * 
   * @param allowed
   *          - argument string you are looking for
   * @return true if argument was provided via the CLI, false otherwise
   */
  public boolean run(String allowed)
  {
    return run(allowed, "t");
  }

  /**
   * Determine if the argument was provided for the selected option, which determines if a test should or should not be run
   * 
   * @param allowed
   *          - argument string you are looking for
   * @param option
   *          - the option of interest
   * @return true if argument was provided via the CLI, false otherwise
   */
  public boolean run(String allowed, String option)
  {
    if (!hasOption(option))
    {
      return true;
    }

    String selection = getOptionValue(option);
    String[] selectionList = selection.split(",");

    for (String selectionItem : selectionList)
    {
      if (selectionItem.equals(allowed))
      {
        return true;
      }
    }

    return false;
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

    // jar file
    Option optionJar = new Option("j", "jar", true, "required -- Fully qualified jar file");
    optionJar.setRequired(false);
    options.addOption(optionJar);

    // test selection
    String tests = "testNum = 1: Wideskies Tests\n";
    tests += "Subtests:\n";
    tests += "E - Elasticsearch MapReduce\n";
    tests += "J - JSON/HDFS MapReduce\n";
    tests += "ES - Elasticsearch Spark \n";
    tests += "JS - JSON/HDFS Spark \n";
    tests += "SS - Spark Streaming Tests \n";
    tests += "JSS - JSON/HDFS Spark Streaming \n";
    tests += "ESS - Elasticsearch Spark Streaming \n";

    Option optionTestSelection = new Option("t", "tests", true, "optional -- Select which tests to execute: \n" + tests);
    optionTestSelection.setRequired(false);
    optionTestSelection.setArgName("<testNum>:<subtestDesignator>");
    optionTestSelection.setType(String.class);
    options.addOption(optionTestSelection);
    return options;
  }

  /**
   * Prints out the help message
   */
  private void printHelp()
  {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(140);
    formatter.printHelp("DistributedTestDriver", cliOptions);
  }
}
