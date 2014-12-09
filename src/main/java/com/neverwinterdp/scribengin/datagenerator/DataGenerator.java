package com.neverwinterdp.scribengin.datagenerator;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.datagenerator.util.PropertyUtils;


// TODO finer granurality, 2 times a second
// TODO Retry mechanism
// TODO auto partition reassignment
// TODO fixed counter e.g ability to write 100000 messages to a topic.
// TODO ability to reconect,
/**
 * The main class
 * */
public class DataGenerator {

  private static final Logger logger = Logger.getLogger(DataGenerator.class);
  private static final Random RANDOM = new Random();
  private int writers;
  private long runPeriod;
  private long delay;
  private String zkURL;
  private String topic;
  private int partitions;


  public static void main(String[] args) {
    BasicConfigurator.configure();
    DataGenerator dataGenerator = new DataGenerator();
    dataGenerator.init();
    try {
      dataGenerator.generate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void init() {
    logger.info("init. ");
    Properties props = PropertyUtils.getPropertyFile("datagenerator.properties");
    writers = Integer.parseInt(props.getProperty("writers"));
    delay = Integer.parseInt(props.getProperty("delay"));
    topic = props.getProperty("topic");
    partitions = Integer.parseInt(props.getProperty("partitions"));
    runPeriod = Integer.parseInt(props.getProperty("run-duration"));
    zkURL = props.getProperty("zookeeper");
    //TODO ensure topics, partitions exists if not create them
  }

  //TODO give the writer a partition
  private void generate() throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
    KafkaProducer writer;

    for (int i = 0; i < writers; i++) {
      writer = new KafkaProducer(zkURL, topic, RANDOM.nextInt(partitions), i);
      final ScheduledFuture<?> timeHandle = scheduler.scheduleAtFixedRate(
          writer, 0, delay, TimeUnit.MILLISECONDS);

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
