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
package org.apache.pirk.storm;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.querier.wideskies.Querier;
import org.apache.pirk.querier.wideskies.QuerierConst;
import org.apache.pirk.querier.wideskies.decrypt.DecryptResponse;
import org.apache.pirk.querier.wideskies.encrypt.EncryptQuery;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.responder.wideskies.storm.*;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.schema.query.filter.StopListFilter;
import org.apache.pirk.schema.response.QueryResponseJSON;
import org.apache.pirk.serialization.LocalFileSystemStore;
import org.apache.pirk.test.utils.BaseTests;
import org.apache.pirk.test.utils.Inputs;
import org.apache.pirk.test.utils.TestUtils;
import org.apache.pirk.utils.SystemConfiguration;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.testing.IntegrationTest;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.TestJob;
import org.json.simple.JSONObject;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.util.*;

@Category(IntegrationTest.class)
public class KafkaStormIntegrationTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaStormIntegrationTest.class);

  private static final LocalFileSystemStore localStore = new LocalFileSystemStore();

  private static TestingServer zookeeperLocalCluster;
  private static KafkaServer kafkaLocalBroker;
  private static ZkClient zkClient;

  private static final String topic = "pirk_test_topic";
  private static final String kafkaTmpDir = "/tmp/kafka";

  private static File fileQuery;
  private static File fileQuerier;
  private static String localStopListFile;

  private QueryInfo queryInfo;
  private BigInteger nSquared;

  @Test
  public void testKafkaStormIntegration() throws Exception
  {
    SystemConfiguration.setProperty("pir.limitHitsPerSelector", "true");
    SystemConfiguration.getProperty("pir.maxHitsPerSelector", "10");
    SystemConfiguration.setProperty("storm.spout.parallelism", "1");
    SystemConfiguration.setProperty("storm.hashbolt.parallelism", "1");
    SystemConfiguration.setProperty("storm.encrowcalcbolt.parallelism", "2");
    SystemConfiguration.setProperty("storm.enccolmultbolt.parallelism", "2");
    SystemConfiguration.setProperty("storm.encrowcalcbolt.ticktuple", "4");
    SystemConfiguration.setProperty("storm.rowDivs", "2");
    SystemConfiguration.setProperty("hdfs.use", "false");

    startZookeeper();
    startKafka();

    SystemConfiguration.setProperty("kafka.topic", topic);
    SystemConfiguration.setProperty("storm.topoName", "pirTest");

    // Create encrypted file
    localStopListFile = Inputs.createPIRStopList(null, false);
    SystemConfiguration.setProperty("pir.stopListFile", localStopListFile);
    Inputs.createSchemaFiles(StopListFilter.class.getName());

    // Perform encryption. Set queryInfo, nSquared, fileQuery, and fileQuerier
    performEncryption();
    SystemConfiguration.setProperty("pir.queryInput", fileQuery.getAbsolutePath());

    KafkaProducer producer = new KafkaProducer<String,String>(createKafkaProducerConfig());
    loadTestData(producer);


    logger.info("Test (splitPartitions,saltColumns) = (true,true)");
    SystemConfiguration.setProperty("storm.splitPartitions", "true");
    SystemConfiguration.setProperty("storm.saltColumns", "true");
    runTest();

    logger.info("Test (splitPartitions,saltColumns) = (true,false)");
    SystemConfiguration.setProperty("storm.splitPartitions", "true");
    SystemConfiguration.setProperty("storm.saltColumns", "false");
    runTest();

    logger.info("Test (splitPartitions,saltColumns) = (false,true)");
    SystemConfiguration.setProperty("storm.splitPartitions", "false");
    SystemConfiguration.setProperty("storm.saltColumns", "true");
    runTest();

    logger.info("Test (splitPartitions,saltColumns) = (false,false)");
    SystemConfiguration.setProperty("storm.splitPartitions", "false");
    SystemConfiguration.setProperty("storm.saltColumns", "false");
    runTest();
  }

  private void runTest() throws Exception
  {
    File responderFile = File.createTempFile("responderFile", ".txt");
    logger.info("Starting topology.");
    runTopology(responderFile);

    // decrypt results
    logger.info("Decrypting results. " + responderFile.length());
    File fileFinalResults = performDecryption(responderFile);

    // check results
    List<QueryResponseJSON> results = TestUtils.readResultsFile(fileFinalResults);
    BaseTests.checkDNSHostnameQueryResults(results, false, 7, false, Inputs.createJSONDataElements());

    responderFile.deleteOnExit();
    fileFinalResults.deleteOnExit();
  }

  private void runTopology(File responderFile) throws Exception
  {
    MkClusterParam mkClusterParam = new MkClusterParam();
    // The test sometimes fails because of timing issues when more than 1 supervisor set.
    mkClusterParam.setSupervisors(1);

    // Maybe using "withSimulatedTimeLocalCluster" would be better to avoid worrying about timing.
    Config conf = PirkTopology.createStormConf();
    conf.put(StormConstants.OUTPUT_FILE_KEY, responderFile.getAbsolutePath());
    conf.put(StormConstants.N_SQUARED_KEY, nSquared.toString());
    conf.put(StormConstants.QUERY_INFO_KEY, queryInfo.toMap());
    // conf.setDebug(true);
    mkClusterParam.setDaemonConf(conf);

    TestJob testJob = createPirkTestJob(conf);
    Testing.withLocalCluster(mkClusterParam, testJob);
    // Testing.withSimulatedTimeLocalCluster(mkClusterParam, testJob);
  }

  private TestJob createPirkTestJob(final Config config)
  {
    final SpoutConfig kafkaConfig = setUpTestKafkaSpout(config);
    return new TestJob()
    {
      StormTopology topology = PirkTopology.getPirkTopology(kafkaConfig);

      @Override
      public void run(ILocalCluster iLocalCluster) throws Exception
      {
        iLocalCluster.submitTopology("pirk_integration_test", config, topology);
        logger.info("Pausing for setup.");
        //Thread.sleep(4000);
        //KafkaProducer producer = new KafkaProducer<String,String>(createKafkaProducerConfig());
        //loadTestData(producer);
        Thread.sleep(6000);
        OutputBolt.latch.await();
        logger.info("Finished...");
      }
    };
  }

  private SpoutConfig setUpTestKafkaSpout(Config conf)
  {
    ZkHosts zkHost = new ZkHosts(zookeeperLocalCluster.getConnectString());

    SpoutConfig kafkaConfig = new SpoutConfig(zkHost, topic, "/pirk_test_root", "pirk_integr_test_spout");
    kafkaConfig.scheme = new SchemeAsMultiScheme(new PirkHashScheme(conf));
    logger.info("KafkaConfig initialized...");

    return kafkaConfig;
  }

  private void startZookeeper() throws Exception
  {
    logger.info("Starting zookeeper.");
    zookeeperLocalCluster = new TestingServer();
    zookeeperLocalCluster.start();
    logger.info("Zookeeper initialized.");

  }

  private void startKafka() throws Exception
  {
    FileUtils.deleteDirectory(new File(kafkaTmpDir));

    Properties props = new Properties();
    props.setProperty("zookeeper.session.timeout.ms", "100000");
    props.put("advertised.host.name", "localhost");
    props.put("port", 11111);
    // props.put("broker.id", "0");
    props.put("log.dir", kafkaTmpDir);
    props.put("enable.zookeeper", "true");
    props.put("zookeeper.connect", zookeeperLocalCluster.getConnectString());
    KafkaConfig kafkaConfig = KafkaConfig.fromProps(props);
    kafkaLocalBroker = new KafkaServer(kafkaConfig, new SystemTime(), scala.Option.apply("kafkaThread"));
    kafkaLocalBroker.startup();

    zkClient = new ZkClient(zookeeperLocalCluster.getConnectString(), 60000, 60000, ZKStringSerializer$.MODULE$);
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperLocalCluster.getConnectString()), false);
    //ZkUtils zkUtils = ZkUtils.apply(zookeeperLocalCluster.getConnectString(), 60000, 60000, false);
    AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties());
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    zkClient.close();
    kafkaLocalBroker.shutdown();
    zookeeperLocalCluster.stop();

    FileUtils.deleteDirectory(new File(kafkaTmpDir));

    fileQuery.delete();
    fileQuerier.delete();

    new File(localStopListFile).delete();
  }

  private HashMap<String,Object> createKafkaProducerConfig()
  {
    String kafkaHostName = "localhost";
    Integer kafkaPorts = 11111;
    HashMap<String,Object> config = new HashMap<String,Object>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName + ":" + kafkaPorts);
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return config;
  }

  private void loadTestData(KafkaProducer producer)
  {
    for (JSONObject dataRecord : Inputs.createJSONDataElements())
    {
      logger.info("Sending record to Kafka " + dataRecord.toString());
      producer.send(new ProducerRecord<String,String>(topic, dataRecord.toString()));
    }
  }

  private void performEncryption() throws Exception
  {
    // ArrayList<String> selectors = BaseTests.selectorsDomain;
    ArrayList<String> selectors = new ArrayList<>(Arrays.asList("s.t.u.net", "d.e.com", "r.r.r.r", "a.b.c.com", "something.else", "x.y.net"));
    String queryType = Inputs.DNS_HOSTNAME_QUERY;

    Paillier paillier = new Paillier(BaseTests.paillierBitSize, BaseTests.certainty);

    nSquared = paillier.getNSquared();

    queryInfo = new QueryInfo(BaseTests.queryIdentifier, selectors.size(), BaseTests.hashBitSize, BaseTests.hashKey, BaseTests.dataPartitionBitSize, queryType,
        false, true, false);

    // Perform the encryption
    logger.info("Performing encryption of the selectors - forming encrypted query vectors:");
    EncryptQuery encryptQuery = new EncryptQuery(queryInfo, selectors, paillier);
    encryptQuery.encrypt(1);
    logger.info("Completed encryption of the selectors - completed formation of the encrypted query vectors:");

    // Write out files.
    fileQuerier = File.createTempFile("pir_integrationTest-" + QuerierConst.QUERIER_FILETAG, ".txt");
    fileQuery = File.createTempFile("pir_integrationTest-" + QuerierConst.QUERY_FILETAG, ".txt");

    localStore.store(fileQuerier.getAbsolutePath(), encryptQuery.getQuerier());
    localStore.store(fileQuery, encryptQuery.getQuery());
  }

  private File performDecryption(File responseFile) throws Exception
  {
    File finalResults = File.createTempFile("finalFileResults", ".txt");
    String querierFilePath = fileQuerier.getAbsolutePath();
    String responseFilePath = responseFile.getAbsolutePath();
    String outputFile = finalResults.getAbsolutePath();
    int numThreads = 1;

    Response response = localStore.recall(responseFilePath, Response.class);
    Querier querier = localStore.recall(querierFilePath, Querier.class);

    // Perform decryption and output the result file
    DecryptResponse decryptResponse = new DecryptResponse(response, querier);
    decryptResponse.decrypt(numThreads);
    decryptResponse.writeResultFile(outputFile);
    return finalResults;
  }

}
