package com.neverwinterdp.kafkaproducer.reader;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import java.util.concurrent.atomic.AtomicInteger;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.retry.DefaultRetryStrategy;
import com.neverwinterdp.kafkaproducer.servers.EmbeddedCluster;
import com.neverwinterdp.kafkaproducer.util.TestLabel;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;
import com.neverwinterdp.kafkaproducer.writer.TestKafkaWriter;

public class TestKafkaReader {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  private static final AtomicInteger integer = new AtomicInteger(0);
  private static final Logger logger = Logger.getLogger(TestKafkaWriter.class);
  private static String zkURL;
  private static EmbeddedCluster cluster;
  private static ZookeeperHelper helper;

  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private KafkaReader reader;
  private String topic;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    printRunningThreads();
    // one zk, 1 kafka
    cluster = new EmbeddedCluster(1, 1);
    cluster.start();
    zkURL = cluster.getZkURL();
    helper = new ZookeeperHelper(zkURL);
    Thread.sleep(3000);
  }

  @Before
  public void setUp() throws Exception {
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);

    reader = new KafkaReader(zkURL, topic, 0);
  }


  @Test
  public void testReadWhileWriting() throws Exception {
    int kafkaPort = helper.getLeaderForTopicAndPartition(topic, 0).getPort();

    Writer writer = new Writer(topic, kafkaPort);
    int writers = 20;
    int delay = 1;
    int runDuration = 20;
    for (int i = 0; i < writers; i++) {

      final ScheduledFuture<?> timeHandle =
          scheduler.scheduleAtFixedRate(writer, 0, delay, TimeUnit.SECONDS);


      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
        }
      }, runDuration, TimeUnit.SECONDS);


    }
    int expected = writers * ((runDuration / delay) + 1);
    List<String> messages = new LinkedList<String>();
    reader.setRetryStrategy(new DefaultRetryStrategy(runDuration, 1000, null));

    while (reader.hasNext()) {
      messages.addAll(reader.read());
    }

    assertEquals(expected, messages.size());
  }

  @Test
  public void testReadAllMessages() throws Exception {
    int kafkaPort = helper.getLeaderForTopicAndPartition(topic, 0).getPort();
    int count = 10;
    Set<Integer> expected = TestUtils.createRange(0, count);
    TestUtils.writeRandomData(topic, kafkaPort, count);
    List<String> messages = new LinkedList<String>();
    while (reader.hasNext()) {
      messages.addAll(reader.read());
    }
    Set<Integer> actual = new TreeSet<>(TestUtils.convert(messages));
    assertEquals(expected, actual);
  }

  @Test
  public void testReadThread() throws Exception {
    int kafkaPort = helper.getLeaderForTopicAndPartition(topic, 0).getPort();
    int writes = 200000;
    TestUtils.writeRandomData(topic, kafkaPort, writes);

    List<String> messages = new ArrayList<>();

    while (reader.hasNext()) {
      messages.addAll(reader.read());
    }
    assertEquals(writes, messages.size());
  }

  @Test
  public void testReadManyThread() throws Exception {
    int kafkaPort = helper.getLeaderForTopicAndPartition(topic, 0).getPort();
    int writes = 200000;
    int partitions = 3;
    helper.addPartitions(topic, partitions);

    TestUtils.writeRandomData(topic, kafkaPort, writes);
    List<KafkaReader> readers = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      reader = new KafkaReader(zkURL, topic, i);
      readers.add(reader);
    }

    int count = 0;
    for (KafkaReader reader : readers) {
      while (reader.hasNext())
        count += reader.read().size();
    }
    assertEquals(writes, count);
  }

  @Test
  public void testReadFromCorrectPartition() throws Exception {}

  @Test(expected = IllegalArgumentException.class)
  public void testReadFromNonExistentPartition() {
    // read from partition 20
    int partition = 20;
    reader = new KafkaReader(zkURL, topic, partition);
    reader.read();
  }

  @Test
  public void testHasNext() throws Exception {
    int kafkaPort = helper.getLeaderForTopicAndPartition(topic, 0).getPort();

    TestUtils.writeRandomData(topic, kafkaPort, 1);

    assertTrue(reader.hasNext());
  }

  @Test
  @TestLabel("KW-PT1_1")
  public void testHasNoNext() throws Exception {
    System.out.println("this is what matter most");
    assertFalse(reader.hasNext());
  }

  @Test
  public void testReadAll() throws Exception {
    int kafkaPort = helper.getLeaderForTopicAndPartition(topic, 0).getPort();
    int writes = 10000;
    TestUtils.writeRandomData(topic, kafkaPort, writes);
    Collection<String> messages = new LinkedList<>();
    while (reader.hasNext()) {
      messages.addAll(reader.read());
    }
    assertEquals(messages.size(), writes);
  }

  @After
  public void tearDown() throws Exception {
    reader.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    logger.info("tearDownClass.");
    logger.debug("Ignore All exceptions after this message.");

    helper.deleteKafkaData();
    helper.close();
    cluster.shutdown();

    printRunningThreads();
  }


  class Writer implements Runnable {

    String topic;
    int kafkaPort;

    public Writer(String topic, int kafkaPort) {
      super();
      this.topic = topic;
      this.kafkaPort = kafkaPort;
    }

    @Override
    public void run() {
      Properties props = kafka.utils.TestUtils.getProducerConfig("localhost:" + kafkaPort);
      ProducerConfig config = new ProducerConfig(props);
      Producer<String, byte[]> producer = new Producer<>(config);


      KeyedMessage<String, byte[]> data =
          new KeyedMessage<>(topic, String.valueOf(integer.incrementAndGet()).getBytes());
      List<KeyedMessage<String, byte[]>> messages = new ArrayList<>();
      messages.add(data);
      producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
      producer.close();
      System.out.println("wrote " + integer.intValue());
    }
  }
}
