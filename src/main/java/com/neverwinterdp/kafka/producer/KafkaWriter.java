package com.neverwinterdp.kafka.producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableSet;
import com.neverwinterdp.kafka.producer.generator.MessageGenerator;
import com.neverwinterdp.kafka.producer.generator.SampleMessageGenerator;
import com.neverwinterdp.kafka.producer.util.HostPort;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;

// may not know the partition to read from
// re-get brokers, retry write
public class KafkaWriter implements Runnable, Closeable {

  private static final Logger logger = Logger.getLogger(KafkaWriter.class);
  private static final CharMatcher CHAR_MATCHER = CharMatcher.anyOf("[]");
  private Producer<String, String> producer;

  private String topic;
  private int partition;
  private String zkURL;
  private String message;
  private MessageGenerator<String> messageGenerator;
  private Class<? extends Partitioner> partitionerClass;

  public KafkaWriter(String zkURL, String topic, int partition, int id) throws Exception {
    this.zkURL = zkURL;
    this.topic = topic;
    this.partition = partition;

    messageGenerator = new SampleMessageGenerator(topic, partition, id);
    partitionerClass = messageGenerator.getPartitionerClass();
    createProducer();
  }

  public KafkaWriter(String zkURL, String topic, int id) throws Exception {
    this(zkURL, topic, -1, id);
  }

  //We are being asked to write to a non existent topic/partition. 
  //Do we try to create it or do we die?  
  //TODO where do we get partitions and replication factor from?
  //TODO externalize createTopic()?
  private void createProducer() throws Exception {
    Collection<HostPort> brokers;
    String brokerString;

    try (ZookeeperHelper helper = new ZookeeperHelper(zkURL)) {
      if (helper.getBrokersForTopicAndPartition(topic, 0).size() == 0)
        helper.createTopic(topic, 2, 2);
      brokers = ImmutableSet.copyOf(helper.getBrokersForTopic(topic).values());
    }
    brokerString = CHAR_MATCHER.removeFrom(brokers.toString());
    logger.info("SERVERS: " + brokerString);


    Properties props = new Properties();
    props.put("metadata.broker.list", brokerString);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", partitionerClass.getName());
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<String, String>(config);
  }

  @Override
  public void run() {
    message = messageGenerator.next();
    logger.info(Thread.currentThread().getName() + message);
    try {
      write(message);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }


  //TODO externalize the retry mechanism
  public void write(String message) throws Exception {
    logger.info("writeToKafka.");
    boolean success = false;
    int retries = 0;
    int maxRetries = 5;
    while (!success && retries++ < maxRetries) {
      try {
        String key;
        if (partition != -1) {// We already know the partition we want
          key = Integer.toString(partition);
        } else {
          key = message.substring(message.indexOf("PARTITION"));
        }
        logger.info("KEY: " + key);
        KeyedMessage<String, String> data =
            new KeyedMessage<String, String>(topic, key, message);
        producer.send(data);
        success = true;
      } catch (Exception e) {// TODO narrow the exception
        createProducer();
      }
    }
    if (!success) {
      //the write was not successful after maxRetries retries
    }
  }

  public String getMessage() {
    return message;
  }

  public void setPartitionerClass(Class<? extends Partitioner> clazz) {
    this.partitionerClass = clazz;
  }

  @Override
  public void close() throws IOException {
    producer.close();
  }
}
