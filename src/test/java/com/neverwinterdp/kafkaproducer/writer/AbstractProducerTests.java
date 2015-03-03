package com.neverwinterdp.kafkaproducer.writer;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import kafka.server.KafkaServer;

import org.junit.After;
import org.junit.Before;

import com.neverwinterdp.kafkaproducer.retry.RunnableRetryer;
import com.neverwinterdp.kafkaproducer.servers.EmbeddedCluster;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;
//TODO test actual content not just count of written messages

public abstract class AbstractProducerTests {
  
  static {
    System.setProperty("log4j.configuration",
        "file:src/test/resources/log4j.properties");
  }
  protected int writers = 3;
  protected ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
  protected EmbeddedCluster servers;
  protected ZookeeperHelper helper;
  protected int kafkaBrokers = 3;
  protected String zkURL;
  protected int zkBrokers = 1;


  protected String topic;

  @Before
  public void setUp() throws Exception {
    servers = new EmbeddedCluster(zkBrokers, kafkaBrokers);
    servers.start();
    zkURL = servers.getZkURL();
    helper = new ZookeeperHelper(zkURL);
    topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, kafkaBrokers);
  }

  @After
  public void tearDown() throws Exception {
    scheduler.shutdownNow();
    helper.close();
    servers.shutdown();
    RunnableRetryer.resetCounter();
    Thread.sleep(2500);
  }

  /**
   * Have 5 threads write to a topic partition, while writing kill leader. Check
   * if all messages were written to kafka despite dead leader.
   */
  public abstract void testWriteToFailedLeader() throws Exception;

  /**
   * Have 5 threads write to a topic partition, while writing kill leader and
   * kill the newly elected leader as well. Check if all messages were written
   * to kafka despite dead leaders.
   */
  public abstract void testFailTwoLeaders() throws Exception;

  public abstract void testKillAllBrokers() throws Exception;

  public abstract void testWriteAfterRebalance() throws Exception;

  protected void killLeader() throws Exception {
    // kill the leader
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, 0);
    Iterator<KafkaServer> iterator = servers.getKafkaServers().iterator();
    while (iterator.hasNext()) {
      KafkaServer server = iterator.next();
      if (leader.getHost().equals(server.config().hostName())
          && leader.getPort() == server.config().port()) {
        server.shutdown();
        server.awaitShutdown();
        System.out.println("Shutting down current leader --> "
            + server.config().hostName() + ":" + server.config().port());
   //     iterator.remove();
      }
    }
  }

  protected void restartAllBrokers() {
    for (KafkaServer server : servers.getKafkaServers()) {
      server.startup();
      System.out.println("starting server localhost:" + server.config().port());
    }
  }
}