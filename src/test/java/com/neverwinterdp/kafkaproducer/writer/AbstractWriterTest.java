package com.neverwinterdp.kafkaproducer.writer;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import kafka.common.FailedToSendMessageException;
import kafka.server.KafkaServer;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.reader.KafkaReader;
import com.neverwinterdp.kafkaproducer.retry.DefaultRetryStrategy;
import com.neverwinterdp.kafkaproducer.retry.RunnableRetryer;
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
  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

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

  private void writeAndRead() throws Exception {
    String topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);
    Properties props = initProperties();
    KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).properties(props).build();
    writer.write("message");
    KafkaReader reader = new KafkaReader(zkURL, topic, 0);

    List<String> messages = reader.read();
    assertEquals(messages.size(), 1);
    reader.close();
  }

  protected abstract Properties initProperties() throws Exception;

  @Test(expected = IndexOutOfBoundsException.class)
  public void testNoServerRunning() throws Exception {
    try {
      initCluster(0, 0);
      writeAndRead();
    } finally {
      // cluster.shutdown();
    }

  }

  @Test(expected = ZkNoNodeException.class)
  public void testOnlyZookeeperRunning() throws Exception {
    try {
      initCluster(1, 0);
      writeAndRead();
    } finally {
      cluster.shutdown();
    }
  }

  @Test(expected = ZkTimeoutException.class)
  public void testOnlyBrokerRunning() throws Exception {
    try {
      initCluster(0, 1);
      writeAndRead();
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

  @Test
  public void testWriteToSingleTopicSinglePartition() throws Exception {
    try {
      initCluster(1, 1);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 3, 1);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).properties(props).partition(0).build();
      writer.write("message");
      KafkaReader reader;
      reader = new KafkaReader(zkURL, topic, 0);
      List<String> messages = reader.read();
      assertEquals(messages.size(), 1);
      assertEquals(messages.get(0), "message");
      for (int i = 1; i < 3; i++) {
        reader = new KafkaReader(zkURL, topic, i);
        messages = reader.read();
        assertEquals(messages.size(), 0);
      }
      reader.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testWriteTenThousandMessages() throws Exception {
    try {
      initCluster(1, 1);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 3, 1);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).properties(props).partition(0).build();
      for (int i = 0; i < 10000; i++)
        writer.write("message" + i);
      KafkaReader reader;
      reader = new KafkaReader(zkURL, topic, 0);
      List<String> messages = new LinkedList<String>();
      ;
      while (reader.hasNext()) {
        messages.addAll(reader.read());
      }
      assertEquals(messages.size(), 10000);

      for (int i = 0; i < 10000; i++) {
        assertEquals(messages.get(i), "message" + i);
      }
      reader.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testWriteTenThousandMessagesToFiveTopics() throws Exception {
    try {
      initCluster(1, 1);
      String[] topics = new String[5];
      Properties props = initProperties();
      for (int i = 0; i < 5; i++)
        topics[i] = TestUtils.createRandomTopic();
      for (int i = 0; i < 5; i++) {
        helper.createTopic(topics[i], 1, 1);
        KafkaWriter writer = new KafkaWriter.Builder(zkURL, topics[i]).properties(props).partition(0).build();
        for (int j = 0; j < 10000; j++)
          writer.write("message" + j);
      }
      for (int i = 0; i < 5; i++) {
        KafkaReader reader;
        reader = new KafkaReader(zkURL, topics[i], 0);
        List<String> messages = new LinkedList<String>();

        while (reader.hasNext()) {
          messages.addAll(reader.read());
        }
        assertEquals(messages.size(), 10000);

        for (int j = 0; j < 10000; j++) {
          assertEquals(messages.get(i), "message" + i);
        }
        messages.clear();
        reader.close();
      }

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testWriteTenThousandMessagesToFiveTopicsTowPartition() throws Exception {
    try {
      initCluster(1, 1);
      String[] topics = new String[5];
      for (int i = 0; i < 5; i++)
        topics[i] = TestUtils.createRandomTopic();
      int[] partitions = { 0, 1 };
      Properties props = initProperties();
      for (int i = 0; i < 5; i++) {
        helper.createTopic(topics[i], 2, 1);
        for (int partition : partitions) {
          KafkaWriter writer = new KafkaWriter.Builder(zkURL, topics[i]).properties(props).partition(partition).build();
          for (int j = 0; j < 5000; j++)
            writer.write("message" + j);
        }
      }

      for (int i = 0; i < 5; i++) {
        KafkaReader reader;
        for (int partition : partitions) {
          reader = new KafkaReader(zkURL, topics[i], partition);
          List<String> messages = new LinkedList<String>();

          while (reader.hasNext()) {
            messages.addAll(reader.read());
          }
          assertEquals(messages.size(), 5000);

          for (int j = 0; j < 5000; j++) {
            assertEquals(messages.get(i), "message" + i);
          }

          reader.close();
          messages.clear();
        }
      }

    } finally {
      cluster.shutdown();
    }
  }

  // reader.setRetryStrategy(new DefaultRetryStrategy(20, 1000, null));

  @Test
  public void testRetryUntilKafkaStart() throws Exception {
    try {
      initCluster(1, 0);
      List<String> messages = new ArrayList<>();

      // 6 writers, writing every 2 seconds for 300 seconds
      int delay = 2;
      int runDuration = 20;
      RunnableRetryer retryer;
      final String topic = TestUtils.createRandomTopic();

      KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).build();
      retryer = new RunnableRetryer(new DefaultRetryStrategy(5, 500, FailedToSendMessageException.class), writer);
      final ScheduledFuture<?> timeHandle = scheduler.scheduleWithFixedDelay(retryer, 0, delay, TimeUnit.SECONDS);

      scheduler.scheduleWithFixedDelay(new Runnable() {

        @Override
        public void run() {
          cluster.addKafkaServer();
          helper.createTopic(topic, 1, 1);

        }
      }, 0, 3, TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
          scheduler.shutdown();
        }
      }, runDuration, TimeUnit.SECONDS);

      messages = TestUtils.readMessages(topic, zkURL);
      // int expected = writers * runDuration / delay;
      int expected = RunnableRetryer.getCounter().get();

      assertEquals(expected, messages.size());
    } finally {
      cluster.shutdown();
    }

  }

  @Test
  public void testRetryUntilTopicCreation() throws Exception {
    try {
      initCluster(1, 3);
      List<String> messages = new ArrayList<>();

      // 6 writers, writing every 2 seconds for 300 seconds
      int delay = 2;
      int runDuration = 20;
      RunnableRetryer retryer;
      final String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 1, 1);
      KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).build();
      retryer = new RunnableRetryer(new DefaultRetryStrategy(5, 500, FailedToSendMessageException.class), writer);
      final ScheduledFuture<?> timeHandle = scheduler.scheduleWithFixedDelay(retryer, 0, delay, TimeUnit.SECONDS);

      scheduler.scheduleWithFixedDelay(new Runnable() {

        @Override
        public void run() {
          helper.createTopic(topic, 1, 1);

        }
      }, 0, 3, TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
          scheduler.shutdown();
        }
      }, runDuration, TimeUnit.SECONDS);

      messages = TestUtils.readMessages(topic, zkURL);
      // int expected = writers * runDuration / delay;
      int expected = RunnableRetryer.getCounter().get();

      assertEquals(expected, messages.size());
    } finally {
      cluster.shutdown();
    }

  }

  @Test
  public void testRetryWhenLeaderKilled() throws Exception {
    try {
      initCluster(1, 3);
      List<String> messages = new ArrayList<>();
      RunnableRetryer retryer;
      final String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 1, 3);
      KafkaWriter.Builder builder = new KafkaWriter.Builder(zkURL, topic);
      KafkaWriter writer = new KafkaWriter(builder) {
        public void run()  {
          for (int i = 0; i < 10000; i++) {
            try {
              write("message"+i);
              if(i== 9)
                   killLeader(topic);
            } catch (Exception e) {
              System.out.println("Exception " + e);
              
            }
          }
        }

      };
      retryer = new RunnableRetryer(new DefaultRetryStrategy(5, 500, FailedToSendMessageException.class), writer);
      retryer =
          new RunnableRetryer(
              new DefaultRetryStrategy(5, 500, FailedToSendMessageException.class), writer);
      
      retryer.run();
      messages = TestUtils.readMessages(topic, zkURL);

      assertEquals(10000, messages.size());
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
