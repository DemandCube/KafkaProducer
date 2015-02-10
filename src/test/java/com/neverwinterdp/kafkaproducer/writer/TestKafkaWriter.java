package com.neverwinterdp.kafkaproducer.writer;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import kafka.common.FailedToSendMessageException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.messagegenerator.IntegerGenerator;
import com.neverwinterdp.kafkaproducer.servers.EmbeddedCluster;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class TestKafkaWriter {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  private static final Logger logger = Logger.getLogger(TestKafkaWriter.class);
  private static String zkURL;
  private static EmbeddedCluster cluster;
  private static ZookeeperHelper helper;


  private static KafkaWriter writer;
  private static String topic;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    printRunningThreads();
    // one zk, 3 kafkas
    cluster = new EmbeddedCluster(1, 1);
    cluster.start();
    zkURL = cluster.getZkURL();
    helper = new ZookeeperHelper(zkURL);
    Thread.sleep(3000);
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);

    writer = new KafkaWriter.Builder(zkURL, topic).partition(0).build();
  }

  @Before
  public void setUp() throws Exception {
  
  }

  @Test
  public void testWriteToPartitionZero() throws Exception {
    logger.info("testWriteToPartitionZero. ");
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);
    writer = new KafkaWriter.Builder(zkURL, topic).partition(1).build();
    try {
      for (int i = 0; i < 100; i++) {
        writer.write("my message");
      }
      logger.info("We got here");
    } catch (Exception e) {
      e.printStackTrace();
      fail("couldnt write to kafka " + e);
    }
  }

  @Test
  public void testWriteToPartitionOne() {
    logger.info("testWriteToPartitionOne. ");
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);
    helper.addPartitions(topic, 2);
    try {
      writer = new KafkaWriter.Builder(zkURL, topic).partition(1).build();
      for (int i = 0; i < 100; i++) {
        writer.write("my message");
      }
    } catch (Exception e) {
      fail("couldnt write to kafka " + e);
    }
  }

  /**
   * Write to kafka, write to buffer. Read from kafka, read from buffer. Should be equal.
   */
  @Test
  public void testWriteMessageOrder() throws Exception {
    int count = 20;
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);
    writer = new KafkaWriter.Builder(zkURL, topic).partition(1).build();
    String randomMessage = UUID.randomUUID().toString();
    List<String> messages = new LinkedList<>();
    LinkedList<String> buffer = new LinkedList<>();

    for (int i = 0; i < count; i++) {
      writer.write(randomMessage);
      buffer.add(randomMessage);
    }
    messages = TestUtils.readMessages(topic, zkURL);
    for (String message : messages) {
      assertEquals(message, buffer.poll());
    }
  }

  /**
   * 
   */
  @Test
  public void testWriteManyPartitions() throws Exception {
    // odd numbers to one partition, even to other
    // Read all see if we get all integers
    int count = 20;
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);
    Set<Integer> expected = TestUtils.createRange(0, count);
    helper.addPartitions(topic, 2);
    writer = new KafkaWriter.Builder(zkURL, topic).messageGenerator(new IntegerGenerator()).build();
    for (int i = 0; i < count; i++) {
      writer.run();
    }
    List<String> messages = TestUtils.readMessages(topic, zkURL);
    Set<Integer> actual = new TreeSet<>(TestUtils.convert(messages));
    System.out.println("expectedSize: " + expected.size() + " actualSize:" + actual.size());
    System.out.println("expected " + expected + " actual " + actual);

    assertEquals(expected, actual);

  }


  @After
  public void tearDown() throws Exception {
    writer.close();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    logger.info("tearDownClass.");
    logger.debug("Ignore All exceptions after this message.");

    helper.deleteKafkaData();
    helper.close();
    cluster.shutdown();

    printRunningThreads();
  }
}
