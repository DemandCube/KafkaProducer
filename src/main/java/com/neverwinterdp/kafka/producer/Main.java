package com.neverwinterdp.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.neverwinterdp.kafka.producer.messagegenerator.KafkaInfoTimeStampGenerator;
import com.neverwinterdp.kafka.producer.util.PropertyUtils;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;
import com.neverwinterdp.kafka.producer.writer.KafkaWriter;

/**
 * The main class
 * */
public class Main {

  private static final Logger logger = Logger.getLogger(Main.class);
  private static int writers;
  private static long runPeriod;
  private static long delay;
  private static String zkURL;
  private static String topic;
  private static int partitions;
  private static int replicationFactor;

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    init();
    try {
      generate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void init() throws Exception {
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
  private static void generate() throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(writers);
    
    for (int i = 0; i < writers; i++) {
      KafkaWriterRunnable sched = new KafkaWriterRunnable( 
                                    new KafkaWriter("writer"+Integer.toString(i), zkURL, topic), 
                                    new KafkaInfoTimeStampGenerator(topic, "writer"+Integer.toString(i))
                                  );
      final ScheduledFuture<?> timeHandle =
          scheduler.scheduleAtFixedRate(sched, 0, delay, TimeUnit.SECONDS);

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
