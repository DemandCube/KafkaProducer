package com.neverwinterdp.kafkaproducer.writer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import kafka.common.FailedToSendMessageException;
import kafka.server.KafkaServer;

import org.junit.Test;

import com.neverwinterdp.kafkaproducer.retry.DefaultRetryStrategy;
import com.neverwinterdp.kafkaproducer.retry.RunnableRetryer;
import com.neverwinterdp.kafkaproducer.util.TestUtils;

public class TestKafkaProducer extends AbstractProducerTests {

  private KafkaWriter writer;

  @Test
  @Override
  public void testWriteToFailedLeader() throws Exception {
    List<String> messages = new ArrayList<>();
    // 6 writers, writing every 2 seconds for 300 seconds
    int delay = 1;
    int runDuration = 20;

    RunnableRetryer retryer;
    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter.Builder(zkURL, topic).partition(0).build();
      writer.connect();
      retryer = new RunnableRetryer(new DefaultRetryStrategy(5, 500,
          FailedToSendMessageException.class), writer);
      final ScheduledFuture<?> timeHandle = scheduler.scheduleWithFixedDelay(retryer, 0, delay,
          TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
          scheduler.shutdown();
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    killLeader();
    // wait for all writers to finish writing
    Thread.sleep(runDuration * 3000);

    System.out.println("hopefully we have finished writting everything.");

    messages = TestUtils.readMessages(topic, zkURL);
    // int expected = writers * runDuration / delay;
    int expected = RunnableRetryer.getCounter().get();

    assertEquals(expected, messages.size());
  }

  @Test
  @Override
  public void testFailTwoLeaders() throws Exception {
    List<String> messages = new ArrayList<>();
    // 6 writers, writing every 2 seconds for 300 seconds
    int delay = 2;
    int runDuration = 20;

    RunnableRetryer retryer;
    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter.Builder(zkURL, topic).partition(0).build();
      writer.connect();
      retryer = new RunnableRetryer(new DefaultRetryStrategy(5, 500,
          FailedToSendMessageException.class), writer);
      final ScheduledFuture<?> timeHandle = scheduler
          .scheduleWithFixedDelay(retryer, 0, delay, TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    killLeader();
    // and we also kill the new leader
    killLeader();

    // Wait for all writers to finish writing
    Thread.sleep(runDuration * 3000);

    System.out.println("hopefully we have finished writting everything.");
    messages = TestUtils.readMessages(topic, zkURL);

    // int expected = writers * runDuration / delay;
    int expected = RunnableRetryer.getCounter().get();
    assertEquals(expected, messages.size());
  }

  /**
   * Have 5 threads write to a topic partition, while writing kill all brokers
   * then restart. Check if all messages were written to kafka despite dead
   * leader.
   */
  @Test
  @Override
  public void testKillAllBrokers() throws Exception {

    List<String> messages = new ArrayList<>();
    // 6 writers, writing every 2 seconds for 300 seconds
    int delay = 5;
    int runDuration = 60;

    RunnableRetryer retryer;
    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter.Builder(zkURL, topic).partition(0).build();
      writer.connect();
      retryer = new RunnableRetryer(new DefaultRetryStrategy(5, 1000, FailedToSendMessageException.class), writer);
      final ScheduledFuture<?> timeHandle = scheduler
          .scheduleWithFixedDelay(retryer, 0, delay, TimeUnit.SECONDS);
      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    messages = TestUtils.readMessages(topic, zkURL);
    int expected = RunnableRetryer.getCounter().get();
    //RunnableRetryer.resetCounter();
    assertEquals(expected, messages.size());
    System.err.println("-------> expected " +expected+" messages.size() " + messages.size());
    for (int i = 0; i < kafkaBrokers; i++) {
      killLeader();
    }

    Thread.sleep(3000);
    restartAllBrokers();
    Thread.sleep(3000);
    helper.rebalanceTopic(topic, 0, new ArrayList<Object>(servers.getKafkaServers()));

    System.out.println("sleeping for " + runDuration * 1000 + " ms to wait all writers to write.");
    Thread.sleep(runDuration * 1000);
    System.out.println("we have writen everything.");
    // int expected = writers * runDuration / delay;
    messages = TestUtils.readMessages(topic, zkURL);
    expected = RunnableRetryer.getCounter().get();
    System.err.println("-------> expected " +expected+" messages.size() " + messages.size());
    assertEquals(expected, messages.size());
  }

  /**
   * Write 100 messages, kill 2 brokers, start new servers, rebalance, kill old
   * leader, write 100 messages Assert messages read =300
   * */
  @Test
  @Override
  public void testWriteAfterRebalance() throws Exception {
    int delay = 1;
    int runDuration = 60;
    RunnableRetryer retryer;

    writer = new KafkaWriter.Builder(zkURL, topic).partition(0).build();
    writer.connect();
    retryer = new RunnableRetryer(new DefaultRetryStrategy(5, 1000,
        FailedToSendMessageException.class), writer);
    final ScheduledFuture<?> timeHandle = scheduler.scheduleWithFixedDelay(retryer, 1, delay,
        TimeUnit.SECONDS);
    scheduler.schedule(new Runnable() {
      public void run() {
        timeHandle.cancel(false);
      }
    }, runDuration, TimeUnit.SECONDS);

    System.out.println("brokers before rebalance " + helper.getBrokersForTopic(topic));

    killLeader();
    killLeader();

    servers.startAdditionalBrokers(2);
    List<Object> remainingBrokers = new ArrayList<>();
    for (KafkaServer server : servers.getKafkaServers()) {
      remainingBrokers.add(server.config().brokerId());
    }

    helper.rebalanceTopic(topic, 0, remainingBrokers);

    System.out.println("brokers after rebalance " + helper.getBrokersForTopic(topic));
    Thread.sleep(runDuration * 1000);
    List<String> messages = TestUtils.readMessages(topic, zkURL);
    assertEquals(RunnableRetryer.getCounter().get(), messages.size());
  }

}
