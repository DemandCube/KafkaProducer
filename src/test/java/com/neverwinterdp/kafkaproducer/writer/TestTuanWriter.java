package com.neverwinterdp.kafkaproducer.writer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import kafka.server.KafkaServer;

import org.junit.Test;

import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.writer.tuan.KafkaWriter;

public class TestTuanWriter extends AbstractProducerTests {

  private KafkaWriter writer;

  @Override
  @Test
  public void testWriteToFailedLeader() throws Exception {
    List<String> messages = new ArrayList<>();
    // 6 writers, writing every 1 seconds for 20 seconds
    int delay = 1;
    int runDuration = 20;
    
    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter(topic, getBrokerURL());

      final ScheduledFuture<?> timeHandle = scheduler.scheduleWithFixedDelay(writer, 0, delay,TimeUnit.SECONDS);

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

    System.out.println("we have finished writing everything.");

    messages = TestUtils.readMessages(topic, zkURL);
    int expected = writer.getCounter().get();

    assertEquals(expected, messages.size());
  }

  @Override
  @Test
  public void testFailTwoLeaders() throws Exception {
    List<String> messages = new ArrayList<>();
    // 6 writers, writing every 2 seconds for 300 seconds
    int delay = 1;
    int runDuration = 20;

    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter(topic, getBrokerURL());

      final ScheduledFuture<?> timeHandle = scheduler.scheduleWithFixedDelay(writer, 0, delay,TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
          scheduler.shutdown();
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    killLeader();
    killLeader();

    // wait for all writers to finish writing
    Thread.sleep(runDuration * 3000);

    System.out.println("we have finished writing everything.");

    messages = TestUtils.readMessages(topic, zkURL);
    int expected = writer.getCounter().get();

    assertEquals(expected, messages.size());
  }

  @Override
  @Test
  public void testKillAllBrokers() throws Exception {
    List<String> messages = new ArrayList<>();
    int delay = 1;
    int runDuration = 20;

    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter(topic,  getBrokerURL());

      final ScheduledFuture<?> timeHandle = scheduler.scheduleWithFixedDelay(writer, 0, delay,TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
          scheduler.shutdown();
        }
      }, runDuration, TimeUnit.SECONDS);
    }
    for (int i = 0; i < kafkaBrokers; i++) {
      killLeader();
    }

    Thread.sleep(3000);
    restartAllBrokers();
    // wait for all writers to finish writing
    Thread.sleep(runDuration * 3000);

    System.out.println("we have finished writing everything.");

    messages = TestUtils.readMessages(topic, zkURL);
    int expected = writer.getCounter().get();

    assertEquals(expected, messages.size());
  }

  @Override
  @Test
  public void testWriteAfterRebalance() throws Exception {
    int delay = 1;
    int runDuration = 60;
    writer = new KafkaWriter(topic,  getBrokerURL());

    final ScheduledFuture<?> timeHandle = scheduler.scheduleWithFixedDelay(writer, 0, delay,TimeUnit.SECONDS);
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
    killLeader();

    System.out.println("brokers after rebalance " + helper.getBrokersForTopic(topic));

    List<String> messages = TestUtils.readMessages(topic, zkURL);
    int expected = writer.getCounter().get();

    assertEquals(expected, messages.size());
  }

  private String getBrokerURL() {
    return servers.getKafkaHosts().toString().replace("[", "").replace("]", "");
  }
}
