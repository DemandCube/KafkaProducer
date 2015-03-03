package com.neverwinterdp.kafkaproducer.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.common.FailedToSendMessageException;
import kafka.producer.DefaultPartitioner;
import kafka.producer.Partitioner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.collect.ImmutableSet;
import com.neverwinterdp.kafkaproducer.messagegenerator.DefaultMessageGenerator;
import com.neverwinterdp.kafkaproducer.messagegenerator.MessageGenerator;
import com.neverwinterdp.kafkaproducer.retry.RetryableRunnable;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class KafkaWriter implements RetryableRunnable, Closeable {

  // private static final Logger logger = Logger.getLogger(KafkaWriter.class);

  enum Status {
    WAITING, FAILED, SUCCESS
  }

  enum WriterStatus {
    ACTIF, PAUSE, STOP
  }

  private KafkaProducer producer;
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
  private List<Long> offsets = new ArrayList<Long>();
  private Map<String, Status> messagesStatus = new HashMap<String, Status>();
  private WriterStatus writerStatus = WriterStatus.ACTIF;

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

  public void connect() {

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
        props.put("request.required.acks", "-1");
        props.put("metadata.broker.list", brokerString);
        props.put("bootstrap.servers", brokerString);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", partitionerClass.getName());
        props.putAll(properties);
        producer = new KafkaProducer(props);
        connected = true;
      }

    } catch (Exception e) {
      connected = false;
      e.printStackTrace();

    }

  }

  @Override
  public void run() {
    String message = messageGenerator.next();
    try {
      while (writerStatus.equals(WriterStatus.PAUSE))
        Thread.sleep(1000);

      if (writerStatus.equals(WriterStatus.ACTIF))
        write(message);

      if (writerStatus.equals(WriterStatus.STOP))
        return;

    } catch (Exception e) {
      System.out.println("Exception " + e);
    }
  }

  public void write(final String message) {
    System.out.println("Sending ...");
    String key;
    if (partition != -1) {
      // we already know what partition to write to
      key = Integer.toString(partition);
    } else {
      key = message;
    }
    ProducerRecord record = new ProducerRecord(topic, partition, key.getBytes(), message.getBytes());
    producer.send(record, new Callback() {
    
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {

        if (exception != null) {
          messagesStatus.put(message, Status.FAILED);
        } else {
          if (metadata == null) {
            messagesStatus.put(message, Status.FAILED);
          } else {
            if (offsets.contains(metadata.offset())) {
              messagesStatus.put(message, Status.FAILED);
            } else {
              offsets.add(metadata.offset());
              messagesStatus.remove(message);
            }
          }
        }
      }
    });
    messagesStatus.put(message, Status.WAITING);

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
    if (producer != null)
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

    if (!connected) {
      throw new FailedToSendMessageException("Kafka server is not running", new Throwable());
    }
  }

  @Override
  public void processFailed() {
    for (String message : messagesStatus.keySet()) {
      if (messagesStatus.get(message).equals(Status.FAILED)) {
        write(message);
      }
    }

  }

  @Override
  public int getFailureCount() {
    int count = 0;
    for (Status m : messagesStatus.values()) {
      if (m.equals(Status.FAILED)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public void pause() {
    writerStatus = WriterStatus.PAUSE;
  }

  @Override
  public void resume() {
    writerStatus = WriterStatus.ACTIF;

  }

  @Override
  public void stop() {
    writerStatus = WriterStatus.STOP;
  }
}
