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
package org.apache.pirk.responder.wideskies.storm;

import org.apache.pirk.query.wideskies.Query;
import org.apache.pirk.utils.SystemConfiguration;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.LoggerFactory;

/**
 * Storm topology class for wideskies Pirk implementation
 * <p>
 * 
 */
public class PirkTopology
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PirkTopology.class);

  private static final String kafkaClientId = SystemConfiguration.getProperty("kafka.clientId", "KafkaSpout");
  private static final String brokerZk = SystemConfiguration.getProperty("kafka.zk", "localhost:2181");
  private static final String kafkaTopic = SystemConfiguration.getProperty("kafka.topic", "pirkTopic");
  private static final Boolean forceFromStart = Boolean.parseBoolean(SystemConfiguration.getProperty("kafka.forceFromStart", "false"));

  private static final Boolean useHdfs = Boolean.parseBoolean(SystemConfiguration.getProperty("hdfs.use", "true"));
  private static final String hdfsUri = SystemConfiguration.getProperty("hdfs.uri", "localhost");

  private static final String topologyName = SystemConfiguration.getProperty("storm.topoName", "PirkTopology");
  private static final Integer numWorkers = Integer.parseInt(SystemConfiguration.getProperty("storm.workers", "1"));

  private static final Integer spoutParallelism = Integer.parseInt(SystemConfiguration.getProperty("storm.spout.parallelism", "1"));
  private static final Integer partitionDataBoltParallelism = Integer.parseInt(SystemConfiguration.getProperty("storm.partitiondata.parallelism", "1"));
  private static final Integer encrowcalcboltParallelism = Integer.parseInt(SystemConfiguration.getProperty("storm.encrowcalcbolt.parallelism", "1"));
  private static final Integer enccolmultboltParallelism = Integer.parseInt(SystemConfiguration.getProperty("storm.enccolmultbolt.parallelism", "1"));

  private static final Boolean saltColumns = Boolean.parseBoolean(SystemConfiguration.getProperty("storm.saltColumns", "false"));
  private static final Boolean splitPartitions = Boolean.parseBoolean(SystemConfiguration.getProperty("storm.splitPartitions", "false"));

  private static final String queryFile = SystemConfiguration.getProperty("pir.queryInput");
  private static final String outputPath = SystemConfiguration.getProperty("pir.outputFile");

  public static void runPirkTopology() throws Exception
  {
    // Set up Kafka parameters
    logger.info("Configuring Kafka.");
    String zkRoot = "/" + kafkaTopic + "_pirk_storm";
    BrokerHosts zkHosts = new ZkHosts(brokerZk);
    SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, kafkaTopic, zkRoot, kafkaClientId);
    kafkaConfig.ignoreZkOffsets = forceFromStart;

    // Create conf
    logger.info("Retrieving Query and generating Storm conf.");
    Config conf = createStormConf();
    Query query = StormUtils.getQuery(useHdfs, hdfsUri, queryFile);
    conf.put(StormConstants.N_SQUARED_KEY, query.getNSquared().toString());
    conf.put(StormConstants.QUERY_INFO_KEY, query.getQueryInfo().toMap());

    // Configure this for different types of input data on Kafka.
    kafkaConfig.scheme = new SchemeAsMultiScheme(new PirkHashScheme(conf));

    // Create topology
    StormTopology topology = getPirkTopology(kafkaConfig);

    // Run topology
    logger.info("Submitting Pirk topology to Storm...");
    StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology);

  } // main

  /***
   * Creates Pirk topology: KafkaSpout -> PartitionDataBolt -> EncRowCalcBolt -> EncColMultBolt -> OutputBolt Requires KafkaConfig to initialize KafkaSpout.
   *
   * @param kafkaConfig
   * @return
   */
  public static StormTopology getPirkTopology(SpoutConfig kafkaConfig)
  {
    // Create spout and bolts
    KafkaSpout spout = new KafkaSpout(kafkaConfig);
    PartitionDataBolt partitionDataBolt = new PartitionDataBolt();
    EncRowCalcBolt ercbolt = new EncRowCalcBolt();
    EncColMultBolt ecmbolt = new EncColMultBolt();
    OutputBolt outputBolt = new OutputBolt();

    // Build Storm topology
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(StormConstants.SPOUT_ID, spout, spoutParallelism);

    builder.setBolt(StormConstants.PARTITION_DATA_BOLT_ID, partitionDataBolt, partitionDataBoltParallelism).fieldsGrouping(StormConstants.SPOUT_ID,
        new Fields(StormConstants.HASH_FIELD));

    // TODO: Decide whether to use Resource Aware Scheduler. (If not, get rid of b2 and b3).
    BoltDeclarer b2 = builder.setBolt(StormConstants.ENCROWCALCBOLT_ID, ercbolt, encrowcalcboltParallelism)
        .fieldsGrouping(StormConstants.PARTITION_DATA_BOLT_ID, new Fields(StormConstants.HASH_FIELD))
        .allGrouping(StormConstants.ENCCOLMULTBOLT_ID, StormConstants.ENCCOLMULTBOLT_SESSION_END)
        .addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Integer.parseInt(SystemConfiguration.getProperty("storm.encrowcalcbolt.ticktuple")));

    // b2.setMemoryLoad(5000);
    // b2.setCPULoad(150.0);

    BoltDeclarer b3 = builder.setBolt(StormConstants.ENCCOLMULTBOLT_ID, ecmbolt, enccolmultboltParallelism)
        .fieldsGrouping(StormConstants.ENCROWCALCBOLT_ID, StormConstants.ENCROWCALCBOLT_DATASTREAM_ID,
            new Fields(StormConstants.COLUMN_INDEX_ERC_FIELD, StormConstants.SALT))
        .allGrouping(StormConstants.ENCROWCALCBOLT_ID, StormConstants.ENCROWCALCBOLT_FLUSH_SIG);
    // b3.setMemoryLoad(5000);
    // b3.setCPULoad(500.0);

    builder.setBolt(StormConstants.OUTPUTBOLT_ID, outputBolt, 1).globalGrouping(StormConstants.ENCCOLMULTBOLT_ID, StormConstants.ENCCOLMULTBOLT_ID);

    return builder.createTopology();
  }

  public static Config createStormConf()
  {

    Boolean limitHitsPerSelector = Boolean.parseBoolean(SystemConfiguration.getProperty("pir.limitHitsPerSelector"));
    Integer maxHitsPerSelector = Integer.parseInt(SystemConfiguration.getProperty("pir.maxHitsPerSelector"));
    Integer rowDivisions = Integer.parseInt(SystemConfiguration.getProperty("storm.rowDivs", "1"));

    Config conf = new Config();
    conf.setNumAckers(Integer.parseInt(SystemConfiguration.getProperty("storm.numAckers", numWorkers.toString())));
    conf.setMaxSpoutPending(Integer.parseInt(SystemConfiguration.getProperty("storm.maxSpoutPending", "300")));
    conf.setNumWorkers(numWorkers);
    conf.setDebug(false);
    // conf.setNumEventLoggers(2);

    conf.put(conf.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, Integer.parseInt(SystemConfiguration.getProperty("storm.executor.receiveBufferSize", "1024")));
    conf.put(conf.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, Integer.parseInt(SystemConfiguration.getProperty("storm.executor.sendBufferSize", "1024")));
    conf.put(conf.TOPOLOGY_TRANSFER_BUFFER_SIZE, Integer.parseInt(SystemConfiguration.getProperty("storm.transferBufferSize", "32")));
    conf.put(conf.WORKER_HEAP_MEMORY_MB, Integer.parseInt(SystemConfiguration.getProperty("storm.worker.heapMemory", "750")));
    conf.put(conf.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, Double.parseDouble(SystemConfiguration.getProperty("storm.componentOnheapMem", "128")));

    // Pirk parameters to send to bolts
    conf.put(StormConstants.ALLOW_ADHOC_QSCHEMAS_KEY, SystemConfiguration.getProperty("pir.allowAdHocQuerySchemas", "false").equals("true"));
    conf.put(StormConstants.QSCHEMA_KEY, SystemConfiguration.getProperty("query.schemas"));
    conf.put(StormConstants.DSCHEMA_KEY, SystemConfiguration.getProperty("data.schemas"));
    conf.put(StormConstants.HDFS_URI_KEY, hdfsUri);
    conf.put(StormConstants.QUERY_FILE_KEY, queryFile);
    conf.put(StormConstants.USE_HDFS, useHdfs);
    conf.put(StormConstants.OUTPUT_FILE_KEY, outputPath);
    conf.put(StormConstants.LIMIT_HITS_PER_SEL_KEY, limitHitsPerSelector);
    conf.put(StormConstants.MAX_HITS_PER_SEL_KEY, maxHitsPerSelector);
    conf.put(StormConstants.SPLIT_PARTITIONS_KEY, splitPartitions);
    conf.put(StormConstants.SALT_COLUMNS_KEY, saltColumns);
    conf.put(StormConstants.ROW_DIVISIONS_KEY, rowDivisions);
    conf.put(StormConstants.ENCROWCALCBOLT_PARALLELISM_KEY, encrowcalcboltParallelism);
    conf.put(StormConstants.ENCCOLMULTBOLT_PARALLELISM_KEY, enccolmultboltParallelism);

    return conf;
  }
}
