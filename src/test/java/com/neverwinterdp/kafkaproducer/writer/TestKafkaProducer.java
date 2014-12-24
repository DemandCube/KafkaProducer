package com.neverwinterdp.kafkaproducer.writer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.servers.MyCluster;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class TestKafkaProducer {

  private int writers = 3;
  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
  private MyCluster servers;
  private ZookeeperHelper helper;
  private KafkaWriter writer;
  private String topic;
  private int kafkaBrokers = 3;
  private String zkURL;
  private int zkBrokers = 1;

  @Before
  public void setUp() throws Exception {
    servers = new MyCluster(zkBrokers, kafkaBrokers);
    servers.start();
    zkURL = servers.getZkURL();
    helper = new ZookeeperHelper(zkURL);
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);
  }

  /**
   * Have 5 threads write to a topic partition, while writing kill leader. Check if all messages
   * were writen to kafka despite dead leader.
   */
  @Test
  public void testWriteToFailedLeader() throws Exception {
    List<String> messages = new ArrayList<>();
    int delay = 5;
    int runDuration = 30;
    // 6 writers, writing every 5 seconds for 30 seconds
    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter(zkURL, topic, 0, i);
      final ScheduledFuture<?> timeHandle =
          scheduler.scheduleAtFixedRate(writer, 0, delay, TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    // while writer threads are writing, kill the leader
    /*
     * HostPort leader = helper.getLeaderForTopicAndPartition(topic, partition);
     * for (KafkaServer server : servers.getKafkaServers()) {
     * System.out.println("Hostname: " + server.config().hostName() + " port: "
     * + server.config().port());
     * if (leader.getHost() == server.config().hostName()
     * && leader.getPort() == server.config().port()) {
     * server.shutdown();
     * }
     * }
     */
    // Sleep a bit for all writers to finish writing
    Thread.sleep((runDuration * 1000) + 1000);

    messages = TestUtils.readMessages(topic, zkURL);

    int expected = writers * (((runDuration) / delay) + 1);
    assertEquals(expected, messages.size());
  }



  @After
  public void tearDown() throws Exception {
    scheduler.shutdownNow();
    servers.shutdown();
  }
}
