package com.neverwinterdp.kafkaproducer.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableSet;
import com.neverwinterdp.kafkaproducer.messagegenerator.MessageGenerator;
import com.neverwinterdp.kafkaproducer.messagegenerator.DefaultMessageGenerator;
import com.neverwinterdp.kafkaproducer.retry.RetryableRunnable;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

// TODO externalize properties
public class KafkaWriter implements RetryableRunnable, Closeable {

  // private static final Logger logger = Logger.getLogger(KafkaWriter.class);
  private static final CharMatcher CHAR_MATCHER = CharMatcher.anyOf("[]");

  private Producer<String, String> producer;
  private String zkURL;
  private String topic;
  private int partition;
  private MessageGenerator<String> messageGenerator;
  private Class<? extends Partitioner> partitionerClass;
  private ZookeeperHelper helper;


  @Override
  public void run() {
    System.out.println(Thread.currentThread().getName() + " writing");
    String message = messageGenerator.next();
    try {
      write(message);
    } catch (Exception e) {
      System.out.println("Exception " + e);
      throw e;
    }
  }


  public void write(String message) {
    String key;
    if (partition != -1) {
      // we already know what partition to write to
      key = Integer.toString(partition);
    }
    else {
      key = message;
    }
    KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, message);
    producer.send(data);
  }

  public void setPartitionerClass(Class<? extends Partitioner> clazz) {
    this.partitionerClass = clazz;
  }

  @Override
  public void beforeRetry() {
    System.out.println("we have to retry");
    try {
      reconnect();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void reconnect() throws Exception {
    Collection<HostPort> brokers;
    String brokerString;

    brokers = ImmutableSet.copyOf(helper.getBrokersForTopic(topic).values());
    brokerString = CHAR_MATCHER.removeFrom(brokers.toString());

    Properties props = new Properties();
    props.put("metadata.broker.list", brokerString);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", partitionerClass.getName());
    // 0, the producer never waits for an acknowledgement from the broker
    // 1, the producer gets an acknowledgement after the leader replica has received the data.
    // -1, the producer gets an acknowledgement after all in-sync replicas have received the data.
    props.put("request.required.acks", "-1");

    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<String, String>(config);
  }

  @Override
  public void afterRetry() {}

  @Override
  public void close() throws IOException {
    producer.close();
    helper.close();
  }

  // TODO also accept kafkaBrokerList
  public static class Builder {
    // required
    private final String topic;
    private final String zkURL;

    // optional
    private int partition = -1;
    private MessageGenerator<String> messageGenerator;
    public Class<? extends Partitioner> partitionerClass = kafka.producer.DefaultPartitioner.class;

    public Builder(String zkURL, String topic) {
      this.zkURL = zkURL;
      this.topic = topic;
      messageGenerator = new DefaultMessageGenerator(topic, 0, 0);
    }

    public Builder partition(int partition) {
      this.partition = partition;
      return this;
    }

    public Builder messageGenerator(MessageGenerator<String> messageGenerator) {
      this.messageGenerator = messageGenerator;
      this.partitionerClass = messageGenerator.getPartitionerClass();
      return this;
    }

    public KafkaWriter build() throws Exception {
      return new KafkaWriter(this);
    }
  }

  private KafkaWriter(Builder builder) throws Exception {
    zkURL = builder.zkURL;
    topic = builder.topic;
    partition = builder.partition;
    messageGenerator = builder.messageGenerator;
    partitionerClass = builder.partitionerClass;

    helper = new ZookeeperHelper(zkURL);
    reconnect();
  }
}
