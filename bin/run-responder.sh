#!/bin/bash
###############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
###############################################################################

#This script runs the ResponderDriver

#Jar file - full path to jar
JAR=""
echo ${JAR}

#ResponderDriver class 
RESPONDER_DRIVER="org.apache.pirk.responder.wideskies.ResponderDriver"
echo ${RESPONDER_DRIVER}

##
## CLI Options
##

##
## Required Args
##

#dataInputFormat -- required -- 'base', 'elasticsearch', or 'standalone' -- Specify the input format
DATAINPUTFORMAT=""
echo ${DATAINPUTFORMAT}

#dataSchemas -- required -- Comma separated list of data schema file names
DATASCHEMAS=""
echo ${DATASCHEMAS}

#inputData -- required
#Fully qualified name of input file/directory in hdfs; used if inputFormat = 'base'
INPUTDATA=""
echo ${INPUTDATA} 
 
#outputFile -- required -- Fully qualified name of output file in hdfs
OUTPUTFILE=""
echo ${OUTPUTFILE}

#platform -- required -- 'mapreduce', 'spark', or 'standalone'
#Processing platform technology for the responder                
PLATFORM=""
echo ${PLATFORM} 

#queryInput -- required -- Fully qualified dir in hdfs of Query files
QUERYINPUT=""
echo ${QUERYINPUT}

#querySchemas -- required -- Comma separated list of query schema file names
QUERYSCHEMAS=""
echo ${QUERYSCHEMAS}                                    

##
## Optional Args - Leave empty if not using/not changing default values
##

#allowAdHocQuerySchemas -- 'true' or 'false'
#If true, allows embedded QuerySchemas for a query.
#Defaults to 'false'
ALLOWADHOCQUERYSCHEMAS=""
echo ${ALLOWADHOCQUERYSCHEMAS}

#colMultReduceByKey -- 'true' or 'false' -- Spark only
#If true, uses reduceByKey in performing column multiplication; if false, uses groupByKey -> reduce
#Defaults to 'false' 
COLMULTREDUCEBYKEY=""
echo ${COLMULTREDUCEBYKEY}

#baseInputFormat -- required if baseInputFormat = 'base'
#Full class name of the InputFormat to use when reading in the data - must extend BaseInputFormat
BASEINPUTFORMAT=""
echo ${BASEINPUTFORMAT}

#esQuery -- required if baseInputFormat = 'elasticsearch' -- ElasticSearch query
#if using 'elasticsearch' input format
ESQUERY=""
echo ${ESQUERY}

#esResource --  required if baseInputFormat = 'elasticsearch'
#Requires the format <index>/<type> : Elasticsearch resource where data is read and written to
ESRESOURCE=""
echo ${ESRESOURCE}

#useHDFSLookupTable -- 'true' or 'false' - Whether or not to generate and use the
#hdfs lookup table for modular exponentiation
#Defaults to 'false'
HDFSEXP=""
echo ${HDFSEXP}

#baseQuery -- ElasticSearch-like query if using 'base' input format -
#used to filter records in the RecordReader
#Defaults to ?q=*
BASEQUERY=""
echo ${BASEQUERY}

#limitHitsPerSelector -- 'true' or 'false'
#Whether or not to limit the number of hits per selector
#Defaults to 'true'
LIMITHITSPERSELECTOR=""
echo ${LIMITHITSPERSELECTOR}

#mapreduceMapJavaOpts -- Amount of heap (in MB) to allocate per map task
#Defaults to -Xmx2800m
MRMAPJAVAOPTS=""
echo ${MRMAPJAVAOPTS}

#mapreduceMapMemoryMb -- Amount of memory (in MB) to allocate per map task
#Defaults to 3000
MRMAPMEMORYMB=""
echo ${MRMAPMEMORYMB}
     
#mapreduceReduceJavaOpts
#Amount of heap (in MB) to allocate per reduce task
#Defaults to -Xmx2800m
MRREDUCEJAVAOPTS=""
echo ${MRREDUCEJAVAOPTS}

#mapreduceReduceMemoryMb
#Amount of memory (in MB) to allocate per reduce task
#Defaults to 3000
MRREDUCEMEMORYMB=""
echo ${MRREDUCEMEMORYMB}

#stopListFile -- optional (unless using StopListFilter) -- Fully qualified file in hdfs
#containing stoplist terms; used by the StopListFilter
STOPLISTFILE=""
echo ${STOPLISTFILE}

#useLocalCache -- 'true' or 'false'
#Whether or not to use the local cache for modular exponentiation
#Defaults to 'true'
USELOCALCACHE=""
echo ${USELOCALCACHE}

#useModExpJoin -- 'true' or 'false' -- Spark only
#Whether or not to pre-compute the modular exponentiation table and join it to the data
#partitions when performing the encrypted row calculations
#Defaults to 'false'
USEMODEXPJOIN=""
echo ${USEMODEXPJOIN}

#numReduceTasks -- optional -- Number of reduce tasks
NUMREDUCETASKS=""
echo ${NUMREDUCETASKS}

#numColMultPartitions -- optional, Spark only
#Number of partitions to use when performing  column multiplication
NUMCOLMULTPARTS=""
echo ${NUMCOLMULTPARTS}        

#maxHitsPerSelector --  optional -- Max number of hits encrypted per selector
MAXHITSPERSELECTOR=""
echo ${MAXHITSPERSELECTOR}

#dataParts -- optional -- Number of partitions for the input data
DATAPARTS=""
echo ${DATAPARTS}

#numExpLookupPartitions -- optional -- Number of partitions for the exp lookup table
EXPPARTS=""
echo ${EXPPARTS}
 
##
## Define the command
##

RESPONDER_DRIVER_CMD="java -cp ${JAR} ${RESPONDER_DRIVER} -d ${DATAINPUTFORMAT}  \
 -ds ${DATASCHEMAS} -i ${INPUTDATA} -o ${OUTPUTFILE} -p ${PLATFORM} \
 -q ${QUERYINPUT} -qs ${QUERYSCHEMAS}"
 
# Add the optional args

if [ -n "${NUMREDUCETASKS}" ]; then
    RESPONDER_DRIVER_CMD+=" -nr ${NUMREDUCETASKS}"
fi
if [ -n "${NUMCOLMULTPARTS}" ]; then
    RESPONDER_DRIVER_CMD+=" -numColMultParts ${NUMCOLMULTPARTS}"
fi
if [ -n "${MAXHITSPERSELECTOR}" ]; then
    RESPONDER_DRIVER_CMD+=" -mh ${MAXHITSPERSELECTOR}"
fi
if [ -n "${DATAPARTS}" ]; then
    RESPONDER_DRIVER_CMD+=" -dataParts ${DATAPARTS}"
fi
if [ -n "${EXPPARTS}" ]; then
    RESPONDER_DRIVER_CMD+=" -expParts ${EXPPARTS}"
fi
if [ -n "${ESQUERY}" ]; then
    RESPONDER_DRIVER_CMD+=" -eq ${ESQUERY}"
fi
if [ -n "${ESRESOURCE}" ]; then
    RESPONDER_DRIVER_CMD+=" -er ${ESRESOURCE}"
fi
if [ -n "${HDFSEXP}" ]; then
    RESPONDER_DRIVER_CMD+=" -hdfsExp ${HDFSEXP}"
fi
if [ -n "${BASEQUERY}" ]; then
    RESPONDER_DRIVER_CMD+=" -j ${BASEQUERY}"
fi
if [ -n "${LIMITHITSPERSELECTOR}" ]; then
    RESPONDER_DRIVER_CMD+=" -lh ${LIMITHITSPERSELECTOR}"
fi
if [ -n "${MRMAPJAVAOPTS}" ]; then
    RESPONDER_DRIVER_CMD+=" -mjo ${MRMAPJAVAOPTS}"
fi
if [ -n "${MRMAPMEMORYMB}" ]; then
    RESPONDER_DRIVER_CMD+=" -mm ${MRMAPMEMORYMB}"
fi
if [ -n "${MRREDUCEJAVAOPTS}" ]; then
    RESPONDER_DRIVER_CMD+=" -rjo ${MRREDUCEJAVAOPTS}"
fi
if [ -n "${MRREDUCEMEMORYMB}" ]; then
    RESPONDER_DRIVER_CMD+=" -rm ${MRREDUCEMEMORYMB}"
fi
if [ -n "${STOPLISTFILE}" ]; then
    RESPONDER_DRIVER_CMD+=" -sf ${STOPLISTFILE}"
fi
if [ -n "${USELOCALCACHE}" ]; then
    RESPONDER_DRIVER_CMD+=" -ulc ${USELOCALCACHE}"
fi
if [ -n "${USEMODEXPJOIN}" ]; then
    RESPONDER_DRIVER_CMD+=" -useModExpJoin ${USEMODEXPJOIN}"
fi
if [ -n "${ALLOWADHOCQUERYSCHEMAS}" ]; then
    RESPONDER_DRIVER_CMD+=" -allowEmbeddedQS ${ALLOWADHOCQUERYSCHEMAS}"
fi
if [ -n "${COLMULTREDUCEBYKEY}" ]; then
    RESPONDER_DRIVER_CMD+=" -colMultRBK ${COLMULTREDUCEBYKEY}"
fi
if [ -n "${BASEINPUTFORMAT}" ]; then
    RESPONDER_DRIVER_CMD+=" -bif ${BASEINPUTFORMAT}"
fi
echo ${RESPONDER_DRIVER_CMD}


##
## Execute the ResponderDriver
## Results will be displayed in the log file.
## 

LOG_FILE="LOG_RESPONDER.txt"
echo ${LOG_FILE}

{
echo ${RESPONDER_DRIVER_CMD}
${RESPONDER_DRIVER_CMD}
if [ $? -ne 0 ]
then
echo "ERROR ResponderDriver. SEE LOG."
exit 0
fi
} &> ${LOG_FILE}
