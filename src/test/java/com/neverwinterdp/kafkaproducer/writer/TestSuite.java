package com.neverwinterdp.kafkaproducer.writer;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.Random;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.writer.KafkaWriter;
import com.neverwinterdp.kafkaproducer.messagegenerator.MessageGenerator;
import com.neverwinterdp.kafkaproducer.messagegenerator.SampleMessageGenerator;
import com.neverwinterdp.kafkaproducer.reader.KafkaReader;
import com.neverwinterdp.kafkaproducer.servers.KafkaCluster;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

// Helper for starting the servers.
// It is intended that extenders clear kafka and zookeeper data in their @Before methods
public class TestSuite {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }
  private static KafkaCluster cluster;
  private static ZookeeperHelper helper;
  private LinkedList<String> buffer;
  private MessageGenerator<String> messageGenerator;
  private KafkaWriter writer;
  private KafkaReader reader;
  private static String zkURL = "127.0.0.1:2181";
  private int id = 1;
  private int partition = 1;
  private String topic;


  @BeforeClass
  public static void setUpBefore() throws Exception {
    printRunningThreads();
    // cluster = new EmbededServers("./build/KafkaCluster", 1, 3);
    // cluster.start();
  }

  @Before
  public void setUp() throws Exception {
    buffer = new LinkedList<>();
    helper = new ZookeeperHelper(zkURL);
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    helper.createTopic(topic, 2, 1);

    messageGenerator = new SampleMessageGenerator(topic, partition, 0);
    partition = new Random().nextInt(1);
    messageGenerator = new SampleMessageGenerator(topic, partition, id);
    writer = new KafkaWriter(zkURL, topic, partition, id);
    writer.setMessageGenerator(new SampleMessageGenerator(topic, partition, id));

    reader = new KafkaReader(zkURL, topic, partition);
  }

  /**
   * Write to kafka, write to buffer. Read from kafka, read from buffer. Should be equal.
   */
  @Test
  public void testWriteMessageOrder() throws Exception {
    int count = 20;
    String randomMessage = UUID.randomUUID().toString();
    for (int i = 0; i < count; i++) {
      writer.write(randomMessage);
      buffer.add(randomMessage);
    }
    while (reader.hasNext()) {
      for (String message : reader.read()) {
        assertEquals(message, buffer.poll());
      }
    }
  }

  @After
  public void after() throws Exception {
    writer.close();
    reader.close();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.shutdown();
    printRunningThreads();
  }

}
