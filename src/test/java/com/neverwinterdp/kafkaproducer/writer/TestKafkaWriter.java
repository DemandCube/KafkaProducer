package com.neverwinterdp.kafkaproducer.writer;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.servers.MyCluster;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class TestKafkaWriter {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  private static final Logger logger = Logger.getLogger(TestKafkaWriter.class);
  private static String zkURL;
  private static MyCluster cluster;
  private static ZookeeperHelper helper;

  private LinkedList<String> buffer;
  private KafkaWriter writer;
  private int id = 1;
  private String topic;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    printRunningThreads();
    // one zk, 3 kafkas
    cluster = new MyCluster(1, 3);
    cluster.start();
    zkURL = cluster.getZkURL();
    helper = new com.neverwinterdp.kafkaproducer.util.ZookeeperHelper(zkURL);
    Thread.sleep(3000);
  }

  @Before
  public void setUp() throws Exception {
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);
    buffer = new LinkedList<>();
    writer = new KafkaWriter(zkURL, topic, 0, id);
  }

  @Test
  public void testWriteToPartitionZero() {
    logger.info("testWriteToPartitionZero. ");
    int id = 0;
    try {
      writer = new KafkaWriter(zkURL, topic, 0, id);
      for (int i = 0; i < 100; i++) {
        writer.write("my message");
      }
      logger.info("We got here");
    } catch (Exception e) {
      fail("couldnt write to kafka " + e);
    }
  }

  @Test
  public void testWriteToPartitionOne() {
    logger.info("testWriteToPartitionOne. ");
    int id = 0;

    helper.addPartitions(topic, 2);
    try {
      writer = new KafkaWriter(zkURL, topic, 1, id);
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
    // TODO discuss with team how to handle multiple partitions.
    // currently this test passes for a 1 partition topic
    int count = 20;
    String randomMessage = UUID.randomUUID().toString();
    List<String> messages = new LinkedList<>();
    writer = new KafkaWriter(zkURL, topic, 0, id);
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
   * Write 100 messages to a non existent topic. If no Exception thrown then we are good.
   */
  @Test
  public void testWriteToNonExistentTopic() {
    topic = TestUtils.createRandomTopic();
    int partition = new Random().nextInt(1);
    try {
      writer = new KafkaWriter(zkURL, topic, partition, id);
      for (int i = 0; i < 5; i++) {
        writer.run();
      }
      assertTrue("Can write to auto created topic", true);
    } catch (Exception e) {
      fail("Cannot write to new Topic");
    }
  }

  /**
   * Write 2000 messages to kafka, count number of messages read. They should be equal
   */
  @Test
  public void testCountMessages() {
    List<String> messages = new LinkedList<>();
    int count = 2000;

    try {
      writer = new KafkaWriter(zkURL, topic, 0, id);
      String randomMessage;
      for (int i = 0; i < count; i++) {
        randomMessage = UUID.randomUUID().toString();
        writer.write(randomMessage);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    logger.debug("finished writing to kafka");
    messages = TestUtils.readMessages(topic, zkURL);
    logger.debug("Messages " + messages);
    assertEquals(count, messages.size());
  }



  /**
   * Start 6 writers to same topic/partition. Run them for a while, read from topic and partition.
   * 
   * @throws Exception
   */
  @Test
  public void testManyWriterThreads() throws Exception {
    List<String> messages = new ArrayList<>();
    // 6 writers, writing every 5 seconds for 30 seconds
    int writers = 6;
    int delay = 5;
    int runDuration = 30;
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
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
    // Sleep a bit for all writers to finish writing
    Thread.sleep((runDuration * 1000) + 2000);
    messages = TestUtils.readMessages(topic, zkURL);
    int expected = writers * (((runDuration) / delay) + 1);
    assertEquals(expected, messages.size());
    scheduler.shutdownNow();

  }

  @Test(expected = kafka.common.FailedToSendMessageException.class)
  public void testWriteToNonExistentPartition() throws Exception {
    // create new topic, create writer to partition 20, expect exception
    topic = TestUtils.createRandomTopic();
    writer = new KafkaWriter(zkURL, topic, 20, 1);
    for (int i = 0; i < 100; i++) {
      writer.write(UUID.randomUUID().toString());
    }
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
