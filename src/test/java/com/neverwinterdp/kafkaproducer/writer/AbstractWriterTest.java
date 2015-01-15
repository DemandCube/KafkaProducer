package com.neverwinterdp.kafkaproducer.writer;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import kafka.common.FailedToSendMessageException;
import kafka.server.KafkaServer;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.reader.KafkaReader;
import com.neverwinterdp.kafkaproducer.servers.EmbeddedCluster;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public abstract class AbstractWriterTest {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  protected static final Logger logger = Logger.getLogger(TestKafkaWriter.class);
  protected static EmbeddedCluster cluster;
  protected static ZookeeperHelper helper;
  protected static String zkURL;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    printRunningThreads();

  }

  protected void initCluster(int numOfZkInstances, int numOfKafkaInstances) throws Exception {
    cluster = new EmbeddedCluster(numOfZkInstances, numOfKafkaInstances);
    cluster.start();
    zkURL = cluster.getZkURL();
    helper = new ZookeeperHelper(zkURL);
    Thread.sleep(3000);
  }

  protected abstract Properties initProperties() throws Exception;

  @Test(expected = IndexOutOfBoundsException.class)
  public void testNoServerRunning() throws Exception {
    try {
      initCluster(0, 0);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 3, 0);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(helper.getBrokersForTopic(topic).values(), topic).properties(props)
          .build();

      writer.write("my message");
    } finally {
      // cluster.shutdown();
    }

  }

  @Test(expected = ZkNoNodeException.class)
  public void testOnlyZookeeperRunning() throws Exception {
    try {
      initCluster(1, 0);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 1, 0);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(helper.getBrokersForTopic(topic).values(), topic).properties(props)
          .build();
      writer.write("my message");
    } finally {
      cluster.shutdown();
    }
  }

  @Test(expected = ZkTimeoutException.class)
  public void testOnlyBrokerRunning() throws Exception {
    try {
      initCluster(0, 1);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 1, 0);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(helper.getBrokersForTopic(topic).values(), topic).properties(props)
          .build();
      writer.write("my message");
    } finally {
      // cluster.shutdown();
    }
  }

  @Test
  public void testWriteToNonExistentTopic() throws Exception {
      writeToNonExistentTopic();
  }

  public void writeToNonExistentTopic() throws Exception {
    try {
      initCluster(1, 1);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(zkURL, "someTopic").properties(props).partition(99).build();
      writer.write("my message");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testWriteToWrongServer() throws Exception {

    try {
      initCluster(1, 2);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 2, 1);
      Properties props = initProperties();
      Collection<HostPort> brokers = helper.getBrokersForTopic(topic).values();
      killLeader(topic);
      brokers = helper.getBrokersForTopic(topic).values();
      KafkaWriter writer = new KafkaWriter.Builder(brokers, topic).properties(props).build();
      writer.write("my message");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testCheckWritenDataExistOnPartition() throws Exception {
    try {
      initCluster(1, 1);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 3, 1);
      // helper.addPartitions(topic, 2);
      Properties props = initProperties();
      for (int i = 0; i < 3; i++) {
        KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).properties(props).partition(i).build();
        writer.write("message" + i);
      }

      for (int i = 0; i < 3; i++) {
        Thread.sleep(5000);
        KafkaReader reader = new KafkaReader(zkURL, topic, i);

        List<String> messages = new LinkedList<String>();
        while (reader.hasNext()) {
          messages.addAll(reader.read());
        }
        assertEquals(messages.size(), 1);
        assertEquals("message" + i, messages.get(0));
        reader.close();

      }

    } finally {
      cluster.shutdown();
    }
  }

  protected void killLeader(String topic) throws Exception {
    // while writer threads are writing, kill the leader
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, 0);
    for (KafkaServer server : cluster.getKafkaServers()) {
      if (leader.getHost().equals(server.config().hostName()) && leader.getPort() == server.config().port()) {
        server.shutdown();
        server.awaitShutdown();
        System.out.println("Shutting down current leader --> " + server.config().hostName() + ":"
            + server.config().port());
      }
    }
  }

}
