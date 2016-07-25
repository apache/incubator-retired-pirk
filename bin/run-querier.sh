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

# This script runs the QuerierDriver

#Jar file - full path to jar
JAR=""
echo ${JAR}

#QuerierDriver class 
QUERIER_DRIVER="org.apache.pirk.querier.wideskies.QuerierDriver"
echo ${QUERIER_DRIVER}


##
## CLI Options
##

##
## Required Args
##

#action -- required - 'encrypt' or 'decrypt' -- The action performed by the QuerierDriver
ACTION=""
echo ${ACTION}

#dataSchemas -- required -- Comma separated list of data schema file names
DATASCHEMAS=""
echo ${DATASCHEMAS}

#inputFile - required - Fully qualified file containing input
#The input is either:
#(1) For Encryption: A query file - Contains the query selectors, one per line;
#the first line must be the query number
#OR
#(2) For Decryption: A response file - Contains the serialized Response object
INPUTFILE=""
echo ${INPUTFILE}

#numThreads -- required -- Number of threads to use for encryption/decryption
NUMTHREADS=""
echo ${NUMTHREADS}

#outputFile -- required - Fully qualified file for the result output.
#The output file specifies either:
#(1) For encryption:
#(a) A file to contain the serialized Querier object named: <outputFile>-querier
#AND
#(b) A file to contain the serialized Query object named: <outputFile>-query
#OR
#(2) A file to contain the decryption results where each line is where each line
#corresponds to one hit and is a JSON object with the schema QuerySchema
OUTPUTFILE=""
echo ${OUTPUTFILE}

#querySchemas -- required -- Comma separated list of query schema file names
QUERYSCHEMAS=""
echo ${QUERYSCHEMAS}

##
## Optional Args - Leave empty if not using/not changing default values
##

## Optional, but required for Encryption (ignored if not encrypting)

#bitset -- required for encryption -- Ensure that this bit position is set in the Paillier
#modulus (will generate Paillier moduli until finding one in which this bit is set)
BITSET=""
echo ${BITSET}

#certainty -- required for encryption -- Certainty of prime generation for Paillier
#must be greater than or equal to 128
CERTAINTY=""
echo ${CERTAINTY}

#dataPartitionBitSize -- required for encryption -- Partition bit size in data partitioning
DATAPARTITIONBITSIZE=""
echo ${DATAPARTITIONBITSIZE}

#embedSelector - required for encryption -- 'true' or 'false'
#Whether or not to embed the selector in the results to reduce false positives
#Defaults to 'true'
EMBEDSELECTOR=""
echo ${EMBEDSELECTOR}

#embedQuerySchema - true or false
#Whether or not to embed the QuerySchema in the Query (via QueryInfo)
#Defaults to 'false'
EMBEDQUERYSCHEMA=""
echo ${EMBEDQUERYSCHEMA}
 
#hashBitSize - required for encryption-- Bit size of keyed hash
HASHBITSIZE=""
echo ${HASHBITSIZE}

#hashKey -- required for encryption -- String key for the keyed hash functionality
HASHKEY=""
echo ${HASHKEY}

#useHDFSLookupTable -- required for encryption -- 'true' or 'false'
#Whether or not to generate and use a hdfs modular exponentation lookup table
#Defaults to 'false'
USEHDFSLOOKUP=""
echo ${USEHDFSLOOKUP}

#memLookupTable -- required for encryption -- 'true' or 'false'
#Whether or not to generate and use an in memory modular exponentation lookup table - only for
#standalone/testing right now...
#Defaults to 'false'
MEMLOOKUPTABLE=""
echo ${MEMLOOKUPTABLE}

#paillierBitSize -- required for encryption -- Paillier modulus size N
PAILLIERBITSIZE=""
echo ${PAILLIERBITSIZE}
 
#queryName -- required for encryption -- Name of the query
QUERYNAME=""
echo ${QUERYNAME}

#queryType -- required for encryption
#Type of the query as defined in the 'schemaName' tag of the corresponding query schema file
QUERYTYPE=""
echo ${QUERYTYPE}

#secureRandomAlg -- specify the SecureRandom algorithm
#Defaults to NativePRNG
SECURERANDOMALG=""
echo ${SECURERANDOMALG}

#secureRandomProvider -- specify the SecureRandom provider
#Defaults to SUN
SECURERANDOMPROVIDER=""
echo ${SECURERANDOMPROVIDER}
  
## Optional, but required for Decryption (ignored if not decrypting)

#querierFile -- required for decryption
#Fully qualified file containing the serialized Querier object
QUERIERFILE=""
echo ${QUERIERFILE}

 
##
## Define the command
##

QUERIER_DRIVER_CMD="java -cp ${JAR} ${QUERIER_DRIVER} -a ${ACTION} -ds ${DATASCHEMAS} \
-i ${INPUTFILE} -nt ${NUMTHREADS} -o ${OUTPUTFILE} -qs ${QUERYSCHEMAS}"

# Add the optional args

if [ -n "${BITSET}" ]; then
    QUERIER_DRIVER_CMD+=" -b ${BITSET}"
fi
if [ -n "${CERTAINTY}" ]; then
    QUERIER_DRIVER_CMD+=" -c ${CERTAINTY}"
fi
if [ -n "${DATAPARTITIONBITSIZE}" ]; then
    QUERIER_DRIVER_CMD+=" -dps ${DATAPARTITIONBITSIZE}"
fi
if [ -n "${EMBEDSELECTOR}" ]; then
    QUERIER_DRIVER_CMD+=" -embed ${EMBEDSELECTOR}"
fi
if [ -n "${EMBEDQUERYSCHEMA}" ]; then
    QUERIER_DRIVER_CMD+=" -embedQS ${EMBEDQUERYSCHEMA}"
fi
if [ -n "${HASHBITSIZE}" ]; then
    QUERIER_DRIVER_CMD+=" -hb ${HASHBITSIZE}"
fi
if [ -n "${HASHKEY}" ]; then
    QUERIER_DRIVER_CMD+=" -hk ${HASHKEY}"
fi
if [ -n "${USEHDFSLOOKUP}" ]; then
    QUERIER_DRIVER_CMD+=" -lu ${USEHDFSLOOKUP}"
fi
if [ -n "${MEMLOOKUPTABLE}" ]; then
    QUERIER_DRIVER_CMD+=" -mlu ${MEMLOOKUPTABLE}"
fi
if [ -n "${PAILLIERBITSIZE}" ]; then
    QUERIER_DRIVER_CMD+=" -pbs ${PAILLIERBITSIZE}"
fi
if [ -n "${QUERYNAME}" ]; then
    QUERIER_DRIVER_CMD+=" -qn ${QUERYNAME}"
fi
if [ -n "${QUERYTYPE}" ]; then
    QUERIER_DRIVER_CMD+=" -qt ${QUERYTYPE}"
fi
if [ -n "${SECURERANDOMALG}" ]; then
    QUERIER_DRIVER_CMD+=" -srAlg ${SECURERANDOMALG}"
fi
if [ -n "${SECURERANDOMPROVIDER}" ]; then
    QUERIER_DRIVER_CMD+=" -srProvider ${SECURERANDOMPROVIDER}"
fi
if [ -n "${QUERIERFILE}" ]; then
    QUERIER_DRIVER_CMD+=" -qf ${QUERIERFILE}"
fi
echo ${QUERIER_DRIVER_CMD}




##
## Execute the QuerierDriver
## Results will be displayed in the log file.
## 

LOG_FILE="LOG_QUERIER.txt"
echo ${LOG_FILE}

{
echo ${QUERIER_DRIVER_CMD}
${QUERIER_DRIVER_CMD}
if [ $? -ne 0 ]
then
echo "ERROR QuerierDriver. SEE LOG."
exit 0
fi
} &> ${LOG_FILE}
