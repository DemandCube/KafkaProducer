package com.neverwinterdp.kafka.producer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.neverwinterdp.kafka.producer.util.PropertyUtils;

/**
 * The main class
 * */
public class KafkaProducer {

  private static final Logger logger = Logger.getLogger(KafkaProducer.class);
  private static final Random RANDOM = new Random();
  private int writers;
  private long runPeriod;
  private long delay;
  private String zkURL;
  private String topic;
  private int partitions;

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    KafkaProducer dataGenerator = new KafkaProducer();
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
    runPeriod = Integer.parseInt(props.getProperty("run-duration"));
    zkURL = props.getProperty("zookeeper");
    //TODO ensure topics, partitions exists if not create them
  }

  //TODO assign partitions to writers in round robin
  private void generate() throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
    KafkaWriter writer;

    for (int i = 0; i < writers; i++) {
      writer = new KafkaWriter(zkURL, topic, RANDOM.nextInt(partitions), i);
      final ScheduledFuture<?> timeHandle = scheduler.scheduleAtFixedRate(
          writer, 0, delay, TimeUnit.SECONDS);

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
