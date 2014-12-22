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

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.reader.KafkaReader;
import com.neverwinterdp.kafkaproducer.servers.KafkaCluster;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

// Start a zookeeper and at least 2 kafka brokers before running these tests
public class TestKafkaWriter {

  private ScheduledExecutorService scheduler;
  private LinkedList<String> buffer;
  private KafkaWriter writer;
  private KafkaReader reader;
  private static String zkURL = "127.0.0.1:2181";
  private static ZookeeperHelper helper;
  private int id = 1;
  private int partition = 1;
  private String topic;
  private int writers;
  private static KafkaCluster cluster;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    printRunningThreads();
    cluster = new KafkaCluster("./build/KafkaCluster", 1, 3);
    cluster.start();
    helper = new ZookeeperHelper(zkURL);
    Thread.sleep(3000);
  }

  @Before
  public void setUp() throws Exception {
    BasicConfigurator.configure();
    writers = 6;
    scheduler = Executors.newScheduledThreadPool(writers);
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    buffer = new LinkedList<>();
    writer = new KafkaWriter(zkURL, topic, partition, id);
  }

  /**
   * Write 100 messages to a non existent topic. If no Exception thrown then we are good.
   */
  @Test
  public void testWriteToNonExistentTopic() throws Exception {
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    partition = new Random().nextInt(1);
    writer = new KafkaWriter(zkURL, topic, partition, id);
    try(ZookeeperHelper helper = new ZookeeperHelper(zkURL)){
      helper.createTopic(topic, 1, 1);
    }
    try {
      for (int i = 0; i < 5; i++) {
        writer.run();
      }
      assertTrue("Can write to auto created topic", true);
    } catch (Exception e) {
      fail("Cannot write to new Topic");
    }
  }

  /**
   * Write 200 messages to kafka, count number of messages read. They should be equal
   */
  @Test
  @Ignore
  public void testCountMessages() {
    List<String> messages = new ArrayList<>();
    int count = 200;
    for (int i = 0; i < count; i++) {
      writer.run();
    }
    while (reader.hasNext()) {
      messages.addAll(reader.read());
    }
    assertEquals(count, messages.size());
  }

  /**
   * Write to kafka, write to buffer. Read from kafka, read from buffer. Should be equal.
   */
  @Test
  @Ignore
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

  /**
   * Start 6 writers to same topic/partition. Run them for a while, read from topic and partition.
   */
  @Test
  @Ignore
    public void testManyWriterThreads() throws Exception {
    List<String> messages = new ArrayList<>();
    // 6 writers, writing every 5 seconds for 30 seconds
    int writers = 6;
    int delay = 5;
    int runDuration = 30;

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
    // Sleep a bit for all writers to finish writing
    Thread.sleep((runDuration * 1000) + 1000);
    reader = new KafkaReader(zkURL, topic, partition);
    while (reader.hasNext()) {
      messages.addAll(reader.read());
    }
    int expected = writers * (((runDuration) / delay) + 1);
    assertEquals(expected, messages.size());
    writer.close();
    reader.close();
  }

  @Test
  @Ignore
  public void testWriteToCorrectPartition() throws Exception {
    // create new topic, create writer to partition0, write, read from partition1 get exception
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    writer = new KafkaWriter(zkURL, topic, 0, 1);
    for (int i = 0; i < 100; i++) {
      writer.run();
    }
    reader = new KafkaReader(zkURL, topic, 1);
    assertTrue(reader.read().size() == 0);

    writer.close();
    reader.close();
  }

  @Test(expected = org.apache.zookeeper.KeeperException.NoNodeException.class)
@Ignore
  public void testReadFromNonExistentPartition() throws Exception {
    // create new topic, create writer to partition0, write, read from partition1 get exception
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    writer = new KafkaWriter(zkURL, topic, 0, 1);
    for (int i = 0; i < 100; i++) {
      writer.run();
    }
    reader = new KafkaReader(zkURL, topic, 5);

    writer.close();
    reader.close();
  }

  @Test(expected = org.apache.zookeeper.KeeperException.NoNodeException.class)
  @Ignore
  public void testWriteToWrongPartition() throws Exception {
    // create new topic, create writer to partition7, expect exception
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    writer = new KafkaWriter(zkURL, topic, 20, 1);
    for (int i = 0; i < 100; i++) {
      writer.write(UUID.randomUUID().toString());
    }
  }

  @After
  public void tearDown() throws Exception {
    writer.close();
   // reader.close();
    scheduler.shutdownNow();
    cluster.shutdown();
  }
}
