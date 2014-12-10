package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.consumer.KafkaReader;
import com.neverwinterdp.kafka.producer.servers.Server;
import com.neverwinterdp.kafka.producer.servers.Servers;
import com.neverwinterdp.kafka.producer.util.HostPort;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;

public class TestKafkaProducer {

  private Servers servers;
  private ZookeeperHelper helper;
  private KafkaWriter writer;
  private KafkaReader reader;
  private int partition = 1;
  private int id = 0;
  private String topic="test";
  private String dataDir = "./build";
  private int kafkaBrokers = 1;
  private int zkPort = 2181;
  private int kafkaPort = 9091;

  private String zkURL = "127.0.0.1:" + zkPort;

  @Before
  public void setUp() throws Exception {
    BasicConfigurator.configure();
    servers = new Servers(dataDir, zkPort, kafkaPort, kafkaBrokers);
    servers.start();
    reader = new KafkaReader(zkURL, topic, partition, id);
    helper = new ZookeeperHelper(zkURL);
  }

  @Test
  public void testWriteToFailedLeader() throws Exception {

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    List<String> messages = new ArrayList<>();

    int count = 5;
    int delay = 2;
    for (int i = 0; i < count; i++) {
      writer = new KafkaWriter(zkURL, topic, partition, i);
      scheduler.scheduleAtFixedRate(writer, 0, delay, TimeUnit.SECONDS);
    }
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, count);
    for (Server server : servers.getKafkaServers()) {
      if (leader.getHost() == server.getHost() && leader.getPort() == server.getPort()) {
        server.shutdown();
      }
    }
    while (reader.hasNext()) {
      messages.add(reader.read());
    }
    assertEquals(count * delay, messages.size());
  }

  @After
  public void tearDown() throws Exception {
    servers.shutdown();
  }
}
