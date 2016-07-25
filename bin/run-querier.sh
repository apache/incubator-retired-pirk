#!/bin/bash
# This script runs the QuerierDriver

#Jar file - full path to jar
JAR=""
echo ${JAR}

#QuerierDriver class 
QUERIER_DRIVER="org.apache.pirk.querier.wideskies.QuerierDriver"
ile"
echo ${QUERIER_DRIVER}


#CLI Options

#action -- required - 'encrypt' or 'decrypt' -- The action performed by the QuerierDriver
ACTION=""
echo ${ACTION}

#bitset -- required for encryption -- Ensure that this bit position is set in the Paillier
#modulus (will generate Paillier moduli until finding one in which this bit is set)
BITSET=""
echo ${BITSET}

#certainty -- required for encryption -- Certainty of prime generation for Paillier
#must be greater than or equal to 128
CERTAINTY=""
echo ${CERTAINTY}



 
 -c,--certainty <certainty>                                  required for encryption -- Certainty of prime generation for Paillier -- must
                                                             be greater than or equal to 128
 -dps,--dataPartitionBitSize <dataPartitionBitSize>          required for encryption -- Partition bit size in data partitioning
 -ds,--dataSchemas <dataSchemas>                             required -- Comma separated list of data schema file names
 -embed,--embedSelector <embedSelector>                      required for encryption -- 'true' or 'false' - Whether or not to embed the
                                                             selector in the results to reduce false positives
 -embedQS,--embedQuerySchema <embedQuerySchema>              optional (defaults to false) -- Whether or not to embed the QuerySchema in the
                                                             Query (via QueryInfo)
 -h,--help                                                   Print out the help documentation for this command line execution
 -hb,--hashBitSize <hashBitSize>                             required -- Bit size of keyed hash
 -hk,--hashKey <hashKey>                                     required for encryption -- String key for the keyed hash functionality
 -i,--inputFile <inputFile>                                  required - Fully qualified file containing input --
                                                             The input is either:
                                                             (1) For Encryption: A query file - Contains the query selectors, one per line;
                                                             the first line must be the query number
                                                             OR
                                                             (2) For Decryption: A response file - Contains the serialized Response object
 -lu,--useHDFSLookupTable <useHDFSLookupTable>               required for encryption -- 'true' or 'false' -- Whether or not to generate and
                                                             use a hdfs modular exponentation lookup table
 -mlu,--memLookupTable <memLookupTable>                      required for encryption -- 'true' or 'false' - Whether or not to generate and
                                                             use an in memory modular exponentation lookup table - only for
                                                             standalone/testing right now...
 -nt,--numThreads <numThreads>                               required -- Number of threads to use for encryption/decryption
 -o,--outputFile <outputFile>                                required - Fully qualified file for the result output.
                                                             The output file specifies either:
                                                             (1) For encryption:
                                                             (a) A file to contain the serialized Querier object named: <outputFile>-querier
                                                             AND
                                                             (b) A file to contain the serialized Query object named: <outputFile>-query
                                                             OR
                                                             (2) A file to contain the decryption results where each line is where each line
                                                             corresponds to one hit and is a JSON object with the schema QuerySchema
 -pbs,--paillierBitSize <paillierBitSize>                    required for encryption -- Paillier modulus size N
 -qf,--querierFile <querierFile>                             required for decryption -- Fully qualified file containing the serialized
                                                             Querier object
 -qn,--queryName <queryName>                                 required for encryption -- Name of the query
 -qs,--querySchemas <querySchemas>                           required -- Comma separated list of query schema file names
 -qt,--queryType <queryType>                                 required for encryption -- Type of the query as defined in the 'schemaName' tag
                                                             of the corresponding query schema file
 -srAlg,--secureRandomAlg <secureRandomAlg>                  optional - specify the SecureRandom algorithm, defaults to NativePRNG
 -srProvider,--secureRandomProvider <secureRandomProvider>   optional - specify the SecureRandom provider, defaults to SUN

#Define the command
QUERIER_DRIVER_CMD="java -cp ${JAR} ${QUERIER_DRIVER} -a ${ACTION} -b ${BITSET} -c ${CERTAINTY} "
echo ${QUERIER_DRIVER_CMD}

# Define the log file
LOG_FILE="LOG_${DATE}.txt"
echo ${LOG_FILE}

#This command will execute each job and determine status. 
#Results will be displayed in the log file. 
{
echo ${JOB_DISTRIBUTION_CMD}
${JOB_DISTRIBUTION_CMD}
if [ $? -ne 0 ]
then
echo "ERROR distribution JOB. SEE LOG."
${CLEANUP_CMD_DIST}
exit $?
fi
${CLEANUP_CMD_DIST}


#echo ${JOB_DOMAINALERT_CMD}
#${JOB_DOMAINALERT_CMD}
#if [ $? -ne 0 ]
#then
#echo "ERROR bfDomainAlert JOB. SEE LOG."
#${CLEANUP_CMD_DOMAINALERT}
#exit $?
#fi

#echo ${JOB_DOMAINALERT_DIFF_CMD}
#${JOB_DOMAINALERT_DIFF_CMD}
#if [ $? -ne 0 ]
#then
#echo "ERROR bfDomainAlert diff JOB. SEE LOG."
#exit $?
#fi


echo ${JOB_MXALERT_CMD}
${JOB_MXALERT_CMD}
if [ $? -ne 0 ]
then
echo "ERROR mxAlert JOB. SEE LOG."
${CLEANUP_CMD_MXALERT}
fi

echo ${JOB_MXALERT_DIFF_CMD}
${JOB_MXALERT_DIFF_CMD}
if [ $? -ne 0 ]
then
echo "ERROR mxDomainAlert diff JOB. SEE LOG."
fi

echo ${JOB_DOMAINDORKING_CMD}
${JOB_DOMAINDORKING_CMD}
${CLEANUP_CMD_DOMAINDORKING}
if [ $? -ne 0 ]
then
echo "ERROR domainDorking JOB. SEE LOG."
${CLEANUP_CMD_DOMAINDORKING}
fi

echo ${JOB_VPS_CMD}
${JOB_VPS_CMD}
${CLEANUP_CMD_VPS}
if [ $? -ne 0 ]
then
echo "ERROR VPS detection JOB. SEE LOG."
${CLEANUP_CMD_VPS}
fi

echo ${JOB_MMG_CMD}
${JOB_MMG_CMD}
${CLEANUP_CMD_MMG}
if [ $? -ne 0 ]
then
echo "ERROR MMG profiling JOB. SEE LOG."
${CLEANUP_CMD_MMG}
fi

exit 0
} &> ${LOG_FILE}
