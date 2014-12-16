package com.neverwinterdp.kafka.producer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
import com.neverwinterdp.kafka.producer.util.HostPort;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;

// may not know the partition to read from
// re-get brokers, retry write
// Callers are responsible for ensuring that topic/partition exist and MessageGenerator is defined
public class KafkaWriter implements Runnable, Closeable {

  private static final Logger logger = Logger.getLogger(KafkaWriter.class);
  private static final CharMatcher CHAR_MATCHER = CharMatcher.anyOf("[]");
  private Producer<String, String> producer;

  private String topic;
  private int partition;
  private MessageGenerator<String> messageGenerator;
  private Class<? extends Partitioner> partitionerClass;
  private ZookeeperHelper helper;

  public KafkaWriter(String zkURL, String topic, int partition, int id) throws Exception {
    checkNotNull(zkURL);
    this.topic = checkNotNull(topic);
    this.partition = checkNotNull(partition);

    helper = new ZookeeperHelper(zkURL);
    checkArgument(helper.getBrokersForTopicAndPartition(topic, 0).size() != 0);
    init();
  }

  public KafkaWriter(String zkURL, String topic, int id) throws Exception {
    this(zkURL, topic, -1, id);
  }

  private void init() throws Exception {
    Collection<HostPort> brokers;
    String brokerString;

    brokers = ImmutableSet.copyOf(helper.getBrokersForTopic(topic).values());
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

  public KafkaWriter setMessageGenerator(MessageGenerator<String> messageGenerator) {
    this.messageGenerator = messageGenerator;
    partitionerClass = messageGenerator.getPartitionerClass();
    return this;
  }


  @Override
  public void run() {
    String message = messageGenerator.next();
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

    String key;
    if (partition != -1) {// We already know the partition we want
      key = Integer.toString(partition);
    } else {
      key = message.substring(message.indexOf("PARTITION"));
    }
    logger.info("KEY: " + key);
    KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, message);
    producer.send(data);
  }

  public void setPartitionerClass(Class<? extends Partitioner> clazz) {
    this.partitionerClass = clazz;
  }

  @Override
  public void close() throws IOException {
    producer.close();
    helper.close();
  }
}
