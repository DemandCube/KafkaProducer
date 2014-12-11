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
import com.neverwinterdp.kafka.producer.servers.KafkaCluster;
import com.neverwinterdp.kafka.producer.util.HostPort;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;

public class TestKafkaProducer {

  private int writers = 3;
  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
  private KafkaCluster servers;
  private ZookeeperHelper helper;
  private KafkaWriter writer;
  private KafkaReader reader;
  private int partition = 1;
  private int id = 0;
  private String topic;
  private String dataDir = "/tmp/verytemp";
  private int kafkaBrokers = 3;
  private int zkPort = 2181;
  private int kafkaPort = 9091;

  private String zkURL = "127.0.0.1:" + zkPort;

  @Before
  public void setUp() throws Exception {
    BasicConfigurator.configure();
    /* servers = new KafkaCluster(dataDir, zkPort, kafkaPort, kafkaBrokers);
     servers.start();*/
    helper = new ZookeeperHelper(zkURL);
    //Get it to work for Existing topic
    topic= Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

  @Test
  public void testWriteToFailedLeader() throws Exception {
    List<String> messages = new ArrayList<>();
    int delay = 5;
    int runDuration = 30;
    //6 writers, writing every 5 seconds for 30 seconds   
    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter(zkURL, topic, partition, i);
      final ScheduledFuture<?> timeHandle = scheduler.scheduleAtFixedRate(
          writer, 0, delay, TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, partition);
    /* for (Server server : servers.getKafkaServers()) {
       if (leader.getHost() == server.getHost() && leader.getPort() == server.getPort()) {
         server.shutdown();
       }
     }*/
    //Sleep a bit for all writers to finish writing
    Thread.sleep((runDuration * 1000) + 1000);
    reader = new KafkaReader(zkURL, topic, partition);
    reader.initialize();
    while (reader.hasNext()) {
      messages.add(reader.read());
    }
    int expected = writers * (((runDuration) / delay) + 1);
    assertEquals(expected, messages.size());
  }

  @After
  public void tearDown() throws Exception {
    scheduler.shutdownNow();
    //  servers.shutdown();
  }
}
