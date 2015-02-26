package com.neverwinterdp.kafkaproducer.writer;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import kafka.server.KafkaServer;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.neverwinterdp.kafkaproducer.retry.RunnableRetryer;
import com.neverwinterdp.kafkaproducer.servers.EmbeddedCluster;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class TestKafkaCallback {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }
  private static int writers = 2;
  private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
  private static final Logger logger = Logger.getLogger(TestKafkaCallback.class);
  private static String zkURL;
  private static EmbeddedCluster cluster;
  private static ZookeeperHelper helper;
  private DefaultCallback callback = new DefaultCallback();

  private KafkaWriter writer;
  private String topic;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    printRunningThreads();
    // one zk, 3 kafkas
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

    writer = new KafkaWriter.Builder(zkURL, topic).callback(callback).build();
    writer.connect();
  }

  @Test
  public void testCountCallsToCallback() throws InterruptedException {
    logger.info("testCountCallsToCallback ");
    int writes = 100;
    for (int i = 0; i < writes; i++) {
      writer.write("message " + i);
    }
    logger.info("We got here");
    Thread.sleep(3000);
    assertEquals(writes, callback.getIntValue());
  }

  @Test
  public void testCallBackOnFailure() throws Exception {
    // 6 writers, writing every 2 seconds for 300 seconds
    int delay = 2;
    int runDuration = 40;

    ImmutableSet<HostPort> brokers = ImmutableSet.copyOf(helper.getBrokersForTopic(topic)
        .values());

    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter.Builder(brokers, topic).callback(callback).build();
      writer.connect();
      final ScheduledFuture<?> timeHandle = scheduler.scheduleWithFixedDelay(writer, 0, delay,
          TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
          scheduler.shutdown();
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    // wait for all writers to finish writing
    Thread.sleep(runDuration * 5000);

    System.out.println("hopefully we have finished writting everything.");

    int expected = writers * runDuration / delay;
    assertEquals(expected, callback.getIntValue());

  }

  /**
   * @throws Exception
   */
  private void killLeader() throws Exception {
    // while writer threads are writing, kill the leader
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, 0);
    for (KafkaServer server : cluster.getKafkaServers()) {
      if (leader.getHost().equals(server.config().hostName())
          && leader.getPort() == server.config().port()) {
        server.shutdown();
        server.awaitShutdown();
        System.out.println("Shutting down current leader --> "
            + server.config().hostName() + ":"
            + server.config().port());
      }
    }
  }

  // test callback on success
  // test callback retries
  @AfterClass
  public static void tearDown() throws Exception {
    scheduler.shutdownNow();
    helper.close();
    cluster.shutdown();
    RunnableRetryer.resetCounter();
  }
}
