package com.neverwinterdp.scribengin.datagenerator;

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

import com.neverwinterdp.scribengin.datagenerator.util.HostPort;
import com.neverwinterdp.scribengin.datagenerator.util.ZookeeperHelper;



public class KafkaProducer implements Runnable, Closeable {

  private static final Logger logger = Logger.getLogger(KafkaProducer.class);
  private static SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
  private Producer<String, String> producer;
  private AtomicInteger sequenceID;
  private String topic;
  private int partition;
  private int writerId;
  private String zkURL;

  public KafkaProducer(String zkURL, String topic, int partition, int id) throws Exception {
    this.zkURL = zkURL;
    this.topic = topic;
    this.partition = partition;
    this.writerId = id;
    sequenceID = new AtomicInteger(0);
  //  createProducer();
  }

  private void createProducer() throws Exception {
    Properties props = new Properties();
    props.put("metadata.broker.list", getBrokerList());
    // props.put("num.partitions", Integer.toString(partitions));
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    //  props.put("partitioner.class", "com.neverwinterdp.scribengin.fixture.SimplePartitioner");
    props.put("request.required.acks", "0");

    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<String, String>(config);
  }

  private String getBrokerList() throws Exception {
    Collection<HostPort> brokers;
    try (ZookeeperHelper helper = new ZookeeperHelper(zkURL);) {
      brokers = helper.getBrokersForTopicAndPartition(topic, partition);
    }
    return brokers.toString();
  }

  @Override
  public void run() {
    Date now = new Date();
    System.out.println(Thread.currentThread().getName() + " TOPIC: " + topic + " PARTITON: "
        + partition + " WriterID:" + writerId
        + " TIME:" + dateFormat.format(now) + " SEQUENCE:" + sequenceID.incrementAndGet());
    try {
    //  writeToKafka();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  private void writeToKafka() {
    logger.info("createKafkaData. ");

    for (int nEvents = 0; nEvents < 100; nEvents++) {
      String msg = "TOPIC: " + topic + " PARTITON: " + partition + " WriterID:" + writerId
          + " TIME:" + dateFormat.format(new Date());
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
      producer.send(data);
    }
  }

  @Override
  public void close() throws IOException {
    producer.close();
  }
}
