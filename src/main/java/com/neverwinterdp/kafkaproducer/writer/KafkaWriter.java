package com.neverwinterdp.kafkaproducer.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import kafka.producer.DefaultPartitioner;
import kafka.producer.Partitioner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.ImmutableSet;
import com.neverwinterdp.kafkaproducer.messagegenerator.DefaultMessageGenerator;
import com.neverwinterdp.kafkaproducer.messagegenerator.MessageGenerator;
import com.neverwinterdp.kafkaproducer.retry.RetryableRunnable;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class KafkaWriter implements RetryableRunnable, Closeable {

  // private static final Logger logger = Logger.getLogger(KafkaWriter.class);
  //TODO get a mechanism to stop writer on error

  private KafkaProducer<String, String> producer;
  private String zkURL;
  private String topic;
  private int partition;
  private MessageGenerator<String> messageGenerator;
  private Class<? extends Partitioner> partitionerClass;
  private ZookeeperHelper helper;
  private Properties properties;
  private Collection<HostPort> brokerList;
  private Callback callback;

  public KafkaWriter(Builder builder) throws Exception {
    zkURL = builder.zkURL;
    brokerList = builder.brokerList;
    if (zkURL != null) {
      helper = new ZookeeperHelper(zkURL);
    }
    topic = builder.topic;
    partition = builder.partition;
    messageGenerator = builder.messageGenerator;
    partitionerClass = builder.partitionerClass;
    properties = builder.properties;
    callback = builder.callback;
  }

  public void connect() throws Exception {
    Collection<HostPort> brokers;
    String brokerString;
    if (zkURL != null)
      brokers = ImmutableSet.copyOf(helper.getBrokersForTopic(topic)
          .values());
    else {
      brokers = brokerList;
    }
    if (brokers.size() == 0) {
      throw new IllegalArgumentException(
          "Provided no brokers to write to or Zookeeper did not provide any brokers for the topic.");
    }

    brokerString = brokers.toString().replace("[", "").replace("]", "");

    Properties props = new Properties();

    // 0, the producer never waits for an acknowledgement from the broker
    // 1, the producer gets an acknowledgement after the leader replica has received the data.
    // -1, the producer gets an acknowledgement after all in-sync replicas have received the data.
    props.put("request.required.acks", "1");
    //for new producer type of bootstrap.servers is list not string
    props.put("bootstrap.servers", brokerString);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("partitioner.class", partitionerClass.getName());
    props.putAll(properties);

    producer = new KafkaProducer<String, String>(props);
  }

  @Override
  public void run() {
    String message = messageGenerator.next();
    try {
      write(message);
    } catch (Exception e) {
      throw e;
    }
  }

  public void write(String message) {
    System.out.println("atempting to write " + message);
    String key;
    if (partition != -1) {
      // we already know what partition to write to
      key = Integer.toString(partition);
    } else {
      key = message;
    }
    ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, key, message);
    producer.send(data, callback);
  }

  public void setPartitionerClass(Class<? extends Partitioner> clazz) {
    this.partitionerClass = clazz;
  }

  @Override
  public void beforeRetry() {
    System.out.println("we have to retry");
    try {
      connect();
    } catch (Exception e) {
      // e.printStackTrace();
    }
  }

  @Override
  public void afterRetry() {
  }

  @Override
  public void close() throws IOException {
    if (producer != null)
      producer.close();
    if (helper != null)
      helper.close();
  }

  // TODO add partitioner
  public static class Builder {
    // required
    private final String topic;

    // Caller must provide one of the two.
    private Collection<HostPort> brokerList;
    private String zkURL;

    // optional
    private int partition = -1;
    private Properties properties = new Properties();
    private MessageGenerator<String> messageGenerator;
    public Class<? extends Partitioner> partitionerClass = DefaultPartitioner.class;
    private Callback callback = new DefaultCallback();;

    // TODO clean up the message generator
    public Builder(String zkURL, String topic) {
      this.zkURL = zkURL;
      this.topic = topic;
      messageGenerator = new DefaultMessageGenerator(topic, 0, 0);
    }

    public Builder(Collection<HostPort> brokerList, String topic) {
      this.brokerList = brokerList;
      this.topic = topic;
      messageGenerator = new DefaultMessageGenerator(topic, 0, 0);
    }

    public Builder partition(int partition) {
      this.partition = partition;
      return this;
    }

    public Builder brokerList(Collection<HostPort> brokerList) {
      this.brokerList = brokerList;
      return this;
    }

    public Builder properties(Properties properties) {
      this.properties = properties;
      return this;
    }

    public Builder callback(Callback callBack) {
      this.callback = callBack;
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
}
