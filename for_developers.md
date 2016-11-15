---
title: For Developers
nav: nav_developers
---

1. [General Info](#general-info)
2. [Code Repo](#code-repo)
3. [Coding Standards](#coding-standards)
	* [Apache License Header](#apache-license-header)
	* [Author Tags](#author-tags)
	* [Code Formatting](#code-formatting)
	* [IDE Configuration Tips](#ide-configuration-tips)
		* [Eclipse](#eclipse)
		* [IntelliJ](#intellij)
4. [JavaDocs](#javadocs)
5. [Building](#building)
6. [Continuous Integration](#continuous-integration)
7. [Testing](#testing)
8. [Debugging](#debugging)
9. [Paillier Benchmarking](#paillier-benchmarking)
10. [Release Process](#release-process)
11. [How To Contribute](#how-to-contribute)

	
## General Info

Pirk is written in Java 8 and build and dependency management is accomplished via [Apache Maven](https://maven.apache.org/). Pirk uses [Git](https://git-scm.com/) for change management. 

## Code Repo

The Pirk code is available via the [Pirk git repository](https://git-wip-us.apache.org/repos/asf?p=incubator-pirk.git) and is mirrored to [Github](https://github.com/apache/incubator-pirk). 

To check out the code: 

	git clone http://git.apache.org/incubator-pirk.git/

Then checkout the 'master' branch (which should be the default): 

	git checkout master

## Coding Standards
 
### Apache License Header

Always add the current ASF license header as described [here](https://www.apache.org/legal/src-headers). Please use the provided ‘eclipse-pirk-template.xml’ code template file to automatically add the ASF header to new code.

### Author Tags 

Please do not use author tags; the code is developed and owned by the community.

### Code Formatting

Pirk follows coding style practices found in the [eclipse-pirk-codestyle.xml](https://github.com/apache/incubator-pirk) file; please ensure that all contributions are formatted accordingly. 

### IDE Configuration Tips

#### Eclipse
* Import Formatter: Properties > Java Code Style > Formatter and import the [eclipse-pirk-codestyle.xml](https://github.com/apache/incubator-pirk) file.
* Import Template: Properties > Java Code Style > Code Templates and import the [eclipse-pirk-template.xml](https://github.com/apache/incubator-pirk). Make sure to check the “Automatically add comments” box. This template adds the ASF header and so on for new code.

#### IntelliJ
* Formatter [plugin](https://code.google.com/p/eclipse-code-formatter-intellij-plugin) that uses eclipse code style xml.

## JavaDocs

Pirk Javadocs may be found [here]({{ site.baseurl }}/javadocs). 

## Building

Pirk currently follows a simple Maven build with a single level pom.xml. As such, Pirk may be built via ‘mvn package’. 

For convenience, the following POM files are included: 

* pom.xml — Pirk pom file for Hadoop/YARN and Spark platforms
* pom-with-benchmarks.xml — Pirk pom file for running Paillier benchmarking testing

Pirk may be built with a specific pom file via ‘mvn package -f <specificPom.xml>’

## Continuous Integration 

Pirk uses [Jenkins](https://builds.apache.org/) for continuous integration. The build history is available [here](https://builds.apache.org/job/pirk/).

Pirk also uses [Travis CI](https://travis-ci.org/) for continuous integration for Github Pull Requests; you can find the build history [here](https://travis-ci.org/apache/incubator-pirk).

## Testing

JUnit in-memory unit and functional testing is performed by building with ‘mvn package’ or running the tests with ‘mvn test’. Specific tests may be run using the Maven command ‘mvn -Dtest=<testName> test’.

Distributed functional testing may be performed on a cluster with the desired distributed computing technology installed. Currently, distributed implementations include batch processing in Hadoop MapReduce and Spark with inputs from HDFS or Elasticsearch. 

To run all of the distributed functional tests on a cluster, the following ‘hadoop jar’ command may be used:

	hadoop jar <pirkJar> org.apache.pirk.test.distributed.DistributedTestDriver -j <full path to pirkJar>

Specific distributed test suites may be run via providing corresponding command line options. The available options are given by the following command:

	hadoop jar <pirkJar> org.apache.pirk.test.distributed.DistributedTestDriver --help

The Pirk functional tests using Spark run via utilizing the [SparkLauncher](https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/launcher/package-summary.html) via the ‘hadoop jar’ command (not by directly running with ’spark-submit’). 
To run successfully, the ‘spark.home’ property must be set correctly in the ‘pirk.properties’ file; ’spark-home’ is the directory containing ’bin/spark-submit’.

## Debugging

Pirk uses [log4j](http://logging.apache.org/log4j/1.2/) for logging. The log4j.properties file may be edited to turn allow a ‘debug’ log level.

## Paillier Benchmarking 

Pirk includes a benchmarking package leveraging [JMH](http://openjdk.java.net/projects/code-tools/jmh/). Currently, [Paillier benchmarking]({{ site.baseurl }}/javadocs/org/apache/pirk/benchmark/PaillierBenchmark) is enabled in Pirk. 

To build with benchmarks enabled, use: 

	mvn package -f pom-with-benchmarks.xml

To run the benchmarks, use: 

	java -jar target/benchmarks.jar

Optionally, you can reduce the number of times each benchmark is run (default is 10) using the -f flag. For example, to run each benchmark only twice, use: ’java -jar target/benchmarks.jar -f 2’

FYI - Right now this spits out a lot of logging errors as the logger fails to work while benchmarks are running. Ignore the many stack traces and wait for execution to complete to see statistics on the different benchmarks.

## Release Process

Please see [Making Releases]({{ site.baseurl }}/making_releases) and [Verifying Releases]({{ site.baseurl }}/verifying_releases).

## How to Contribute

Please see the [How to Contribute]({{ site.baseurl }}/how_to_contribute) page.
