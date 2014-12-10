package com.neverwinterdp.kafka.producer;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.neverwinterdp.kafka.producer.util.HostPort;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;

public class KafkaWriter implements Runnable, Closeable {

  private static final Logger logger = Logger.getLogger(KafkaWriter.class);
  private static SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSSS");
  private static final CharMatcher CHAR_MATCHER = CharMatcher.anyOf("[]"); // remove control characters
  private Producer<String, String> producer;
  private AtomicInteger sequenceID;
  private String topic;
  private int partition;
  private int writerId;
  private String zkURL;

  public KafkaWriter(String zkURL, String topic, int partition, int id) throws Exception {
    this.zkURL = zkURL;
    this.topic = topic;
    this.partition = partition;
    this.writerId = id;
    sequenceID = new AtomicInteger(0);
    createProducer();
  }

  private void createProducer() throws Exception {
    Properties props = new Properties();
    props.put("metadata.broker.list", getBrokerList());
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "com.neverwinterdp.kafkaproducer.SimplePartitioner");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<String, String>(config);
  }


  private String getBrokerList() throws Exception {
    logger.info("getBrokerList. ");
    Collection<HostPort> brokers;
    String brokerString;
    try (ZookeeperHelper helper = new ZookeeperHelper(zkURL);) {
      brokers = helper.getBrokersForTopicAndPartition(topic, partition);
    }
    brokerString = CHAR_MATCHER.removeFrom(brokers.toString());
    logger.info("SERVERS: " + brokerString);
    return brokerString;
  }

  @Override
  public void run() {
    Date now = new Date();
    String message = " TOPIC: " + topic + ", PARTITION: "
        + partition + ", WriterID:" + writerId
        + ", TIME:" + dateFormat.format(now) + ", SEQUENCE:" + sequenceID.incrementAndGet();

    logger.info(Thread.currentThread().getName() + message);
    try {
      write(message);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  public void write(String message) throws Exception {
    logger.info("writeToKafka.");
    try {
      KeyedMessage<String, String> data =
          new KeyedMessage<String, String>(topic, Integer.toString(partition), message);

      producer.send(data);
    } catch (Exception e) {
      createProducer();
      //TODO then attempt to re-write message
    }
  }

  @Override
  public void close() throws IOException {
    producer.close();
  }
}
