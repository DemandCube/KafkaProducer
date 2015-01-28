package com.neverwinterdp.kafkaproducer.servers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaServer;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class TestEmbeddedCluster {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  @Test
  public void testClusterStarts() {
    EmbeddedCluster cluster = new EmbeddedCluster(1, 1);
    try {
      cluster.start();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unable to start Kafka Cluster");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testZkStarts() {
    EmbeddedCluster cluster = new EmbeddedCluster(1, 1);
    int zkPort;
    try {
      cluster.start();
      zkPort = Iterables.getOnlyElement(cluster.getZkHosts()).getPort();
      assertTrue(TestUtils.isPortOpen(zkPort));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unable to start Kafka Cluster");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testClusterShutdown() {
    EmbeddedCluster cluster = new EmbeddedCluster(1, 1);
    try {
      cluster.start();
      cluster.shutdown();
    } catch (Exception e) {
      Assert.fail("Unable to start/shutdown Kafka Cluster");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  // TODO
  public void testClusterReplicates() {
    EmbeddedCluster cluster = new EmbeddedCluster(1, 3);
    try {
      cluster.start();
    } catch (Exception e) {
      Assert.fail("Unable to start/shutdown Kafka Cluster");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testCountKafkaBrokers() throws Exception {
    int kafkaBrokers = 3;
    EmbeddedCluster cluster = new EmbeddedCluster(1, kafkaBrokers);
    try {
      cluster.start();
      assertEquals(kafkaBrokers, cluster.getKafkaHosts().size());
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSimpleKafkaRebalance() throws Exception {
    // start 4 kafkas
    // create topic with replication 3
    // get count of brokers for topics
    // kill leader
    // count brokers for topic
    int kafkaBrokers = 4;
    int replicationFactor = 3;
    EmbeddedCluster cluster = new EmbeddedCluster(1, kafkaBrokers);
    ZookeeperHelper helper = null;
    String topic = "topic";
    try {
      cluster.start();
      helper = new ZookeeperHelper(cluster.getZkURL());

      helper.createTopic(topic, 1, replicationFactor);
      int brokersForTopic = helper.getBrokersForTopicAndPartition(topic, 0).size();

      // before killing leader they should be equal
      assertEquals(replicationFactor, brokersForTopic);
      HostPort leader = helper.getLeaderForTopicAndPartition(topic, 0);
      List<Object> remainingBrokers = new ArrayList<>();
      killLeader(cluster, leader);

      for (KafkaServer server : cluster.getKafkaServers()) {
        remainingBrokers.add(server.config().brokerId());
      }
      // before rebalance the shouldn't be equal
      brokersForTopic = helper.getBrokersForTopicAndPartition(topic, 0).size();
      assertNotEquals(replicationFactor, brokersForTopic);

      helper.rebalanceTopic(topic, 0, remainingBrokers);
      brokersForTopic = helper.getBrokersForTopicAndPartition(topic, 0).size();

      assertEquals(replicationFactor, brokersForTopic);
    } finally {
      helper.close();
      cluster.shutdown();
    }
  }

  @Test
  /**
   * Start 3 kafka brokers,
   * create a topic with replication factor=3, 
   * kill leader, 
   * start new brokers,
   * re-balance, 
   * count brokers for topic. 
   *   
   * They should be 3.
   * */
  public void testKafkaRebalance() throws Exception {

    int kafkaBrokers = 3;
    int replicationFactor = 3;
    EmbeddedCluster cluster = new EmbeddedCluster(1, kafkaBrokers);
    ZookeeperHelper helper = null;
    String topic = "topic";
    try {
      cluster.start();
      helper = new ZookeeperHelper(cluster.getZkURL());

      helper.createTopic(topic, 1, replicationFactor);
      int brokersForTopic = helper.getBrokersForTopicAndPartition(topic, 0).size();

      assertEquals(replicationFactor, brokersForTopic);
      HostPort leader = helper.getLeaderForTopicAndPartition(topic, 0);

      killLeader(cluster, leader);
      cluster.startAdditionalBrokers(2);

      // get all active brokers
      List<Object> remainingBrokers = new ArrayList<>();
      for (KafkaServer server : cluster.getKafkaServers()) {
        remainingBrokers.add(server.config().brokerId());
      }

      helper.rebalanceTopic(topic, 0, remainingBrokers.subList(0, replicationFactor));

      brokersForTopic = helper.getBrokersForTopicAndPartition(topic, 0).size();
      assertEquals(replicationFactor, brokersForTopic);
    } finally {
      helper.close();
      cluster.shutdown();
    }
  }


  @Test
  public void testKafkaBrokersCanProduce() throws Exception {
    String topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    int kafkaBrokers = 3;
    int kafkaPort;
    EmbeddedCluster cluster = new EmbeddedCluster(1, kafkaBrokers);
    Producer<String, byte[]> producer = null;
    try {
      cluster.start();


      ZookeeperHelper helper = new ZookeeperHelper(cluster.getZkURL());
      helper.createTopic(topic, 2, 2);

      helper.close();
      kafkaPort = Iterables.get(cluster.getKafkaHosts(), 0).getPort();

      Properties props = kafka.utils.TestUtils.getProducerConfig("localhost:" + kafkaPort);
      ProducerConfig config = new ProducerConfig(props);
      producer = new Producer<>(config);

      byte[] testMessage = "testMessage".getBytes(Charsets.UTF_8);
      KeyedMessage<String, byte[]> data = new KeyedMessage<>(topic, testMessage);
      List<KeyedMessage<String, byte[]>> messages = new ArrayList<>();
      messages.add(data);
      for (int i = 0; i < 10; i++) {
        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        System.out.println("we wrote");
      }
      producer.close();
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Stops the kafka host that is leader.
   * 
   * @return the broker that was killed.
   */
  private HostPort killLeader(EmbeddedCluster cluster, HostPort leader) throws Exception {
    HostPort killedLeader = null;
    KafkaServer kafkaServer = null;
    for (KafkaServer server : cluster.getKafkaServers()) {
      if (leader.getHost().equals(server.config().hostName())
          && leader.getPort() == server.config().port()) {
        killedLeader = new HostPort(server.config().hostName(), server.config().port());
        server.shutdown();
        server.awaitShutdown();
        kafkaServer = server;
        System.out.println("Shutting down current leader --> " + server.config().hostName() + ":"
            + server.config().port() + " id " + server.config().brokerId());
      }
    }
    cluster.getKafkaServers().remove(kafkaServer);
    return killedLeader;
  }
}
