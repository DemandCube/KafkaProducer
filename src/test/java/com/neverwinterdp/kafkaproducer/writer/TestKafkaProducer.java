package com.neverwinterdp.kafkaproducer.writer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import kafka.common.FailedToSendMessageException;
import kafka.server.KafkaServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.retry.DefaultRetryStrategy;
import com.neverwinterdp.kafkaproducer.retry.RunnableRetryer;
import com.neverwinterdp.kafkaproducer.servers.EmbeddedCluster;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

// Warning: This test takes a while
public class TestKafkaProducer {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }
  private int writers = 3;
  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
  private EmbeddedCluster servers;
  private ZookeeperHelper helper;
  private KafkaWriter writer;
  private String topic;
  private int kafkaBrokers = 3;
  private String zkURL;
  private int zkBrokers = 1;

  @Before
  public void setUp() throws Exception {
    servers = new EmbeddedCluster(zkBrokers, kafkaBrokers);
    servers.start();
    zkURL = servers.getZkURL();
    helper = new ZookeeperHelper(zkURL);
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 3);
  }

  /**
   * Have 5 threads write to a topic partition, while writing kill leader. Check if all messages
   * were writen to kafka despite dead leader.
   */
  @Test
  //TODO write a test that does the same for two partitions
  public void testWriteToFailedLeader() throws Exception {
    List<String> messages = new ArrayList<>();
    // 6 writers, writing every 5 seconds for 30 seconds
    int delay = 5;
    int runDuration = 120;

    RunnableRetryer retryer;

    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter.Builder(zkURL, topic).build();
      retryer =
          new RunnableRetryer(
              new DefaultRetryStrategy(5, 500, FailedToSendMessageException.class), writer);
      final ScheduledFuture<?> timeHandle =
          scheduler.scheduleAtFixedRate(retryer, 0, delay, TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    // while writer threads are writing, kill the leader
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, 0);
    for (KafkaServer server : servers.getKafkaServers()) {
      if (leader.getHost().equals(server.config().hostName())
          && leader.getPort() == server.config().port()) {
        System.out.println("Shutting down current leader --> " + server.config().hostName() + ":"
            + server.config().port());
        server.shutdown();
      }
    }

    // Sleep a bit for all writers to finish writing
    Thread.sleep((runDuration * 1000) + 3000);
    System.out.println("hopefully we have finished writting everything.");

    messages = TestUtils.readMessages(topic, zkURL);

    int expected = writers * (((runDuration) / delay) + 1);
    assertEquals(expected, messages.size());
  }

  @After
  public void tearDown() throws Exception {
    scheduler.shutdownNow();
    helper.close();
    servers.shutdown();
  }
}
