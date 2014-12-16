package com.neverwinterdp.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.neverwinterdp.kafka.producer.util.PropertyUtils;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;

/**
 * The main class
 * */
public class Main {

  private static final Logger logger = Logger.getLogger(Main.class);
  private int writers;
  private long runPeriod;
  private long delay;
  private String zkURL;
  private String topic;
  private int partitions;
  private int replicationFactor;

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Main dataGenerator = new Main();
    dataGenerator.init();
    try {
      dataGenerator.generate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void init() throws Exception {
    logger.info("init. ");
    Properties props = PropertyUtils.getPropertyFile("kafkaproducer.properties");
    writers = Integer.parseInt(props.getProperty("writers"));
    delay = Integer.parseInt(props.getProperty("delay"));
    topic = props.getProperty("topic");
    partitions = Integer.parseInt(props.getProperty("partitions"));
    replicationFactor = Integer.parseInt(props.getProperty("replication-factor"));
    runPeriod = Integer.parseInt(props.getProperty("run-duration"));
    zkURL = props.getProperty("zookeeper");
    // ensure topics, partitions exists if not create them
    try (ZookeeperHelper helper = new ZookeeperHelper(zkURL)) {
      helper.createTopic(topic, partitions, replicationFactor);
    }
  }

  // assign partitions to writers in round robin
  private void generate() throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
    KafkaWriter writer;
    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter(zkURL, topic, i % partitions, i);
      final ScheduledFuture<?> timeHandle =
          scheduler.scheduleAtFixedRate(writer, 0, delay, TimeUnit.SECONDS);

      scheduler.schedule(new Runnable() {
        public void run() {
          timeHandle.cancel(false);
        }
      }, runPeriod, TimeUnit.SECONDS);
    }
    try {
      scheduler.awaitTermination(runPeriod, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }
    logger.info(scheduler.shutdownNow());
  }

}
