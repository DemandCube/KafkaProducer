package com.neverwinterdp.kafkaproducer.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.DefaultPartitioner;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;

import com.google.common.collect.ImmutableSet;
import com.neverwinterdp.kafkaproducer.messagegenerator.DefaultMessageGenerator;
import com.neverwinterdp.kafkaproducer.messagegenerator.MessageGenerator;
import com.neverwinterdp.kafkaproducer.retry.RetryException;
import com.neverwinterdp.kafkaproducer.retry.RetryableRunnable;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class KafkaWriter implements RetryableRunnable, Closeable {

  // private static final Logger logger = Logger.getLogger(KafkaWriter.class);

  private Producer<String, String> producer;
  private String zkURL;
  private String topic;
  private int partition;
  private MessageGenerator<String> messageGenerator;
  private Class<? extends Partitioner> partitionerClass;
  private ZookeeperHelper helper;
  private Properties properties;
  private Collection<HostPort> brokerList;
  private boolean connected;
  private Collection<HostPort> brokers;
  private HostPort leader;

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
    connect();
  }

  private void connect() {
    try {

      String brokerString;
      if (zkURL != null) {
        brokers = ImmutableSet.copyOf(helper.getBrokersForTopic(topic).values());
      } else {
        brokers = brokerList;
      }
      if (brokers.size() == 0) {
        connected = false;
      } else {
        brokerString = brokers.toString().replace("[", "").replace("]", "");
        Properties props = new Properties();

        // 0, the producer never waits for an acknowledgement from the broker
        // 1, the producer gets an acknowledgement after the leader replica
        // has
        // received the data.
        // -1, the producer gets an acknowledgement after all in-sync replicas
        // have received the data.
        props.put("request.required.acks", "-1");
        props.put("metadata.broker.list", brokerString);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", partitionerClass.getName());

        props.putAll(properties);

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
        connected = true;
      }

    } catch (Exception e) {
      connected = false;
      e.printStackTrace();
      
    }

  }

  private void checkBrockersChange() {
    Collection<HostPort> newBrokers;
    HostPort newLeader = null ;
    try {
      if (zkURL != null)
        newBrokers = ImmutableSet.copyOf(helper.getBrokersForTopic(topic).values());
      else {
        newBrokers = brokerList;
      }
      try{
          newLeader = helper.getLeaderForTopicAndPartition(topic, partition);
      }catch(Exception e){
        
      }
      if (newBrokers.size() != brokers.size()) {
        connect();
      } else {
        if (leader !=null && newLeader !=null && !newLeader.toString().equals(leader.toString())) {
          connect();
          leader = newLeader;
        }
      }
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

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
    checkBrockersChange();
    String key;
    if (partition != -1) {
      // we already know what partition to write to
      key = Integer.toString(partition);
    } else {
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
    connect();
  }

  @Override
  public void afterRetry() {
  }

  @Override
  public void close() throws IOException {
    producer.close();
    if (helper != null)
      helper.close();
  }

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

    public Builder messageGenerator(MessageGenerator<String> messageGenerator) {
      this.messageGenerator = messageGenerator;
      this.partitionerClass = messageGenerator.getPartitionerClass();
      return this;
    }

    public KafkaWriter build() throws Exception {
      return new KafkaWriter(this);
    }
  }

  @Override
  public void beforeStart() {

    System.out.println("Check connection");
    if (!connected) {
      throw new FailedToSendMessageException("Kafka server is not running", new Throwable());
    }

  }
}
