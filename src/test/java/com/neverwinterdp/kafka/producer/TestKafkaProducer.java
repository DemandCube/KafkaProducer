package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.consumer.KafkaReader;
import com.neverwinterdp.kafka.producer.util.HostPort;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;
import com.neverwinterdp.kafka.servers.KafkaCluster;
import com.neverwinterdp.kafka.servers.Server;

public class TestKafkaProducer {

  private int writers = 3;
  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
  private KafkaCluster servers;
  private ZookeeperHelper helper;
  private KafkaWriter writer;
  private KafkaReader reader;
  private int partition = 1;
  private String topic;
  private String dataDir = "./build/";
  private int kafkaBrokers = 3;
  private int zkPort = 2181;
  private String zkURL = "127.0.0.1:" + zkPort;
  private int zkBrokers = 1;

  @Before
  public void setUp() throws Exception {
    BasicConfigurator.configure();
    servers = new KafkaCluster(dataDir, zkBrokers, kafkaBrokers);
    servers.start();
    helper = new ZookeeperHelper(zkURL);
    // Get it to work for Existing topic
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

  /**
   * Have 5 threads write to a topic partition, while writing kill leader. Check if all messages
   * were writen to kafka despite dead leader.
   * */
  @Test
  public void testWriteToFailedLeader() throws Exception {
    List<String> messages = new ArrayList<>();
    int delay = 5;
    int runDuration = 30;
    // 6 writers, writing every 5 seconds for 30 seconds
    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter(zkURL, topic, partition, i);
      final ScheduledFuture<?> timeHandle =
          scheduler.scheduleAtFixedRate(writer, 0, delay, TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    // while writer threads are writing, kill the leader
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, partition);
    for (Server server : servers.getKafkaServers()) {
      if (leader.getHost() == server.getHost() && leader.getPort() == server.getPort()) {
        server.shutdown();
      }
    }
    // Sleep a bit for all writers to finish writing
    Thread.sleep((runDuration * 1000) + 1000);
    reader = new KafkaReader(zkURL, topic, partition);
    while (reader.hasNext()) {
      messages.addAll(reader.read());
    }
    int expected = writers * (((runDuration) / delay) + 1);
    assertEquals(expected, messages.size());
  }

  @After
  public void tearDown() throws Exception {
    scheduler.shutdownNow();
    servers.shutdown();
  }
}
