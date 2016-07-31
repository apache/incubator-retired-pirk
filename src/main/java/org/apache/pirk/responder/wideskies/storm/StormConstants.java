/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.apache.pirk.responder.wideskies.storm;

public class StormConstants
{
  // Topology Components
  public static final String SPOUT_ID = "kafkaspout";
  public static final String PARTITION_DATA_BOLT_ID = "partitiondataBolt";
  public static final String ENCROWCALCBOLT_ID = "encrowcalcbolt";
  public static final String ENCCOLMULTBOLT_ID = "enccolmultbolt";
  public static final String OUTPUTBOLT_ID = "outputbolt";

  // Extra Streams
  public static final String DEFAULT = "default";
  public static final String ENCROWCALCBOLT_DATASTREAM_ID = "encrowcalcbolt_datastream_id";
  public static final String ENCROWCALCBOLT_FLUSH_SIG = "encrowcalcbolt_flush";
  public static final String ENCCOLMULTBOLT_SESSION_END = "enccolmultbolt_sess_end";

  // Tuple Fields
  // From HashBolt (and variants)
  public static final String HASH_FIELD = "hash";
  public static final String PARTIONED_DATA_FIELD = "parData";
  public static final String JSON_DATA_FIELD = "data";
  // From EncRowCalcBolt
  public static final String COLUMN_INDEX_ERC_FIELD = "colIndexErc";
  public static final String ENCRYPTED_VALUE_FIELD = "encRowValue";
  // From EncColMultBolt
  public static final String COLUMN_INDEX_ECM_FIELD = "colIndex";
  public static final String COLUMN_PRODUCT_FIELD = "colProduct";

  // Configuration Keys
  public static final String USE_HDFS = "useHdfs";
  public static final String HDFS_URI_KEY = "hdfsUri";
  public static final String QUERY_FILE_KEY = "queryFile";
  public static final String QUERY_INFO_KEY = "queryInfo";
  public static final String ALLOW_ADHOC_QSCHEMAS_KEY = "allowAdHocQuerySchemas";
  public static final String QSCHEMA_KEY = "qSchema";
  public static final String DSCHEMA_KEY = "dschema";
  public static final String OUTPUT_FILE_KEY = "output";
  public static final String LIMIT_HITS_PER_SEL_KEY = "limitHitsPerSelector";
  public static final String MAX_HITS_PER_SEL_KEY = "maxHitsPerSelector";
  public static final String SALT_COLUMNS_KEY = "saltColumns";
  public static final String ROW_DIVISIONS_KEY = "rowDivisions";
  public static final String SPLIT_PARTITIONS_KEY = "splitPartitions";
  public static final String N_SQUARED_KEY = "nSquared";
  public static final String ENCROWCALCBOLT_PARALLELISM_KEY = "encrowcalcboltPar";
  public static final String ENCCOLMULTBOLT_PARALLELISM_KEY = "enccolmultboltPar";

  public static final String SALT = "salt";
  public static final String FLUSH = "flush";

}
