---
title: For Users
nav: nav_users
---

1. [System Requirements](#system-requirements)
2. [Target Data](#target-data)
3. [Data and Query Schemas](#data-and-query-schemas)
	* [Data Schema](#data-schema)
	* [Query Schema](#query-schema)
4. [Querier](#querier)
5. [Responder](#responder)
	* [Platforms](#platforms)
	* [Target Data](#target-data)
	* [Launching](#launching)
	* [Responder Output](#responder-output)

## System Requirements

Pirk requires JDK 1.8. The remaining system requirements depend on the platform chosen for the Querier and Responder.

## Target Data

Target data refers to the data to be queried. Target data can be read from HDFS, Elasticsearch, or from the local file system, or as input to the encrypted query functionality as a part of a larger workflow. 

Data over which the encrypted query is to be preformed must be transformed into a map of <key,value> pairs; JSON, MapWritable, and Map\<String,Object\> representations are currently used in Pirk to format target data. For a given data input represented as a set of \<key,value\> pairs, the ‘key’ corresponds to the name of the element or field of the data and the ‘value’ is the value of that field in the data.

If the Responder is reading the target data from HDFS, an input format extending Pirk’s [org.apache.pirk.inputformat.hadoop.BaseInputFormat]({{ site.baseurl }}/javadocs/org/apache/pirk/inputformat/hadoop/BaseInputFormat) must be used; BaseInputFormat extends the [Hadoop InputFormat](https://hadoop.apache.org/docs/r2.7.2/api/org/apache/hadoop/mapred/InputFormat.html)\<[Text](https://hadoop.apache.org/docs/r2.6.2/api/org/apache/hadoop/io/Text.html),[MapWritable](https://hadoop.apache.org/docs/r2.6.2/api/org/apache/hadoop/io/MapWritable.html)\>.

## Data and Query Schemas

In order to perform an encrypted query over a target data set, Pirk requires the user to specify a data schema XML file for the target data and a query schema XML file for the query type. Both the Querier and the Responder must have the data and query schema XML files.

### Data Schema

The format of the data schema XML file is as follows:


	 <schema>
	  <schemaName> name of the data schema </schemaName>
	  <element>
	     <name> element name </name>
	      <type> class name or type name (if Java primitive type) of the element </type>
	     <isArray> (optional) whether or not the schema element is an array within the data; defaults to false </isArray>
	     <partitioner> optional - Partitioner class for the element; defaults to primitive java type partitioner </partitioner> 
	  </element>
	 </schema>
	
	Primitive Java types must be one of the following: "byte", "short", "int", "long", "float", "double", "char", "string", "boolean"

A corresponding XSD file may be found [here](https://github.com/apache/incubator-pirk/blob/master/src/main/resources/data-schema.xsd).

Each element of the data is defined by its name, type, whether or not it is an array of objects of the given type, and an optional partitioner class. 

The element type may be one of the Java primitive types given above or may be defined by a custom class. 

The Partitioner class contains the functionality for partitioning the given data element into ‘chunks’ which are used in computing the encrypted query. If no partitioner is specified for an element, it defaults to the [org.apache.pirk.schema.data.partitioner.PrimitiveTypePartitioner]({{ site.baseurl }}/javadocs/org/apache/pirk/schema/data/partitioner/PrimitiveTypePartitioner) and assumes that the element type is one of the allowed primitive Java types (an exception will be thrown if this is not the case). All custom partitioners must implement the [org.apache.pirk.schema.data.partitioner.DataPartitioner]({{ site.baseurl }}/javadocs/org/apache/pirk/schema/data/partitioner/DataPartitioner) interface. There are several implemented Partitioners available in the [org.apache.pirk.schema.data.partitioner]({{ site.baseurl }}/javadocs/org/apache/pirk/schema/data/partitioner) package.

### Query Schema

The format of the query schema XML file is as follows:

	<schema>
	  <schemaName> name of the query schema </schemaName>
	  <dataSchemaName> name of the data schema over which this query is run </dataSchemaName>
	  <selectorName> name of the element in the data schema that will be the selector </selectorName>
	  <elements>
		<name> element name of element in the data schema to include in the query response </name>
	  </elements>
	  <filter> (optional) name of the filter class to use to filter the data </filter>
	  <filterNames>
	       <name> (optional) element name of element in the data schema to apply pre-processing filters </name>
	  </filterNames>
	</schema>

A corresponding XSD file may be found [here](https://github.com/apache/incubator-pirk/blob/master/src/main/resources/query-schema.xsd).

The selectorName is the name of the element in the corresponding data schema that is to be used as the primary selector or indicator for the query (see the [Wideskies paper]({{ site.baseurl }}/papers/wideskies_paper.pdf)). 

The elements field specifies all elements (via \<name\>) within a given piece of data to return as part of the encrypted query.

Optionally, the Responder can perform filtering on the input target data before performing the encrypted query. The filter class must implement the [org.apache.pirk.schema.query.filter.DataFilter]({{ site.baseurl }}/javadocs/org/apache/pirk/schema/query/filter/DataFilter) interface. Specific elements of a piece of input data on which the filter should be applied can be specified via \<filterNames\>; for example, for the [org.apache.pirk.schema.query.filter.StopListFilter]({{ site.baseurl }}/javadocs/org/apache/pirk/schema/query/filter/StopListFilter), filterNames may include the qname if the target data is a set of DNS records.

## Querier

The Querier is currently written to operate in a standalone (non-distributed), multi-threaded mode.

For Wideskies, the user will need to generate the encrypted query vector via the [QuerierDriver]({{ site.baseurl }}/javadocs/org/apache/pirk/querier/wideskies/QuerierDriver).  Options available for query generation are given by:

	java -cp <pirkJar>  org.apache.pirk.querier.wideskies.QuerierDriver —help

The QuerierDriver generates [Query]({{ site.baseurl }}/javadocs/org/apache/pirk/query/wideskies/Query) and [Querier]({{ site.baseurl }}/javadocs/org/apache/pirk/querier/wideskies/Querier) objects and serializes them, respectively, to two files. 

The Query object contains the encrypted query vectors and all information necessary for the Responder to perform the encrypted query. The file containing the Query object should be sent to the Responder platform; it is used as input for the Responder to execute the query. 

The Querier object contains the information necessary for the Querier to decrypt the results of the encrypted query. The file containing the Querier object must not be sent to the Responder as it contains the encryption/decryption keys for the query.

## Responder

### Platforms

The Responder currently operates on the following platforms:

* Standalone, multithreaded (mainly used for testing purposes)
* Spark batch, reading from HDFS or Elasticsearch
* Hadoop MapReduce batch, reading from HDFS or Elasticsearch

The [RoadMap]({{ site.baseurl }}/roadmap) details plans for various streaming implementations. 

Components of the Responder implementations may also be called independently in custom workflows.

### Target Data

Target data is assumed to be in HDFS for the distributed Responder variants and in the local file system for the standalone version. 

### Launching 

The Responder can be launched via the [ResponderDriver]({{ site.baseurl }}/javadocs/org/apache/pirk/responder/wideskies/ResponderDriver).  Options available via the ResponderDriver are given by:

	java -cp <pirkJar> org.apache.pirk.responder.wideskies.ResponderDriver —help

When using the MapReduce implementation, launch the Responder via the following command:

	hadoop jar <pirkJar> org.apache.pirk.responder.wideskies.ResponderDriver <responder options>

When using the Spark implementation, launch the Responder via spark-submit as follows:

	spark-submit <spark options> <pirkJar> org.apache.pirk.responder.wideskies.ResponderDriver <responder options>

### Responder Output

The Responder performs the encrypted query, stores the results in a [Response]({{ site.baseurl }}/javadocs/org/apache/pirk/response/wideskies/Response) object, and serializes this object to the output location specified in the \<responder options\>. For Responder implementations running in Hadoop MapReduce or Spark, this output file is stored in HDFS.

The file containing the serialized Response object should be returned to the Querier for decryption.

