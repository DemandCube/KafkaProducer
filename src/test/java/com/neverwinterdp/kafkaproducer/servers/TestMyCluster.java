package com.neverwinterdp.kafkaproducer.servers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class TestMyCluster {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  @Test
  public void testClusterStarts() {
    MyCluster cluster = new MyCluster(1, 1);
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
    MyCluster cluster = new MyCluster(1, 1);
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
    MyCluster cluster = new MyCluster(1, 1);
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
  public void testClusterRplicates() {
    MyCluster cluster = new MyCluster(1, 3);
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
    MyCluster cluster = new MyCluster(1, kafkaBrokers);
    try {
      cluster.start();
      assertEquals(kafkaBrokers, cluster.getKafkaHosts().size());
    } finally {
      cluster.shutdown();
    }
  }


  @Test
  public void testKafkaBrokersCanProduce() throws Exception {
    String topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    int kafkaBrokers = 3;
    int kafkaPort;
    MyCluster cluster = new MyCluster(1, kafkaBrokers);
    Producer<String, byte[]> producer = null;
    try {
      cluster.start();

      // cluster.createTopic(topic, 2, 2);
      System.out.println("do we get here");
      ZookeeperHelper helper = new ZookeeperHelper(cluster.getZkURL());
      helper.createTopic(topic, 2, 2);
      System.out.println(helper.getBrokersForTopic(topic));
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
}
