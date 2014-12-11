package com.neverwinterdp.kafka.consumer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;

import com.neverwinterdp.kafka.producer.util.HostPort;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;

public class KafkaReader implements Closeable {
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final int TIMEOUT = 10000;
  private static final Logger logger = Logger.getLogger(KafkaReader.class);
  private ZookeeperHelper helper;

  private int currentOffset = 0;
  private SimpleConsumer consumer;
  private String zkURL;
  private String topic;
  private int partition;

  public KafkaReader(String zkURL, String topic, int partition) throws Exception {
    this.zkURL = zkURL;
    this.topic = topic;
    this.partition = partition;
    initialize();
  }

  public void initialize() throws Exception {
    helper = new ZookeeperHelper(zkURL);
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, partition);

    consumer =
        new SimpleConsumer(leader.getHost(), leader.getPort(), TIMEOUT, BUFFER_SIZE,
            getClientName());
  }

  // One offset many messages?
  public String read() {
    FetchRequest req = new FetchRequestBuilder()
        .clientId(getClientName())
        .addFetch(topic, partition, currentOffset, 100000)
        .build();

    FetchResponse resp = consumer.fetch(req);
    byte[] bytes = null;
    for (MessageAndOffset messageAndOffset : resp.messageSet(topic, partition)) {
      ByteBuffer payload = messageAndOffset.message().payload();
      bytes = new byte[payload.limit()];
      payload.get(bytes);
      logger.info("Concatenating for tmp string: " + String.valueOf(messageAndOffset.offset())
          + ": " + new String(bytes));
    }
    currentOffset++;
    return new String(bytes);
  }

  public boolean hasNext() {
    FetchRequest req = new FetchRequestBuilder()
        .clientId(getClientName())
        .addFetch(topic, partition, currentOffset, 100000)
        .build();
    FetchResponse resp = consumer.fetch(req);
    try {
      resp.messageSet(topic, partition).iterator().next().nextOffset();
    } catch (NoSuchElementException e) {
      return false;
    }
    return true;
  }

  private String getClientName() {
    return topic + "_" + partition;
  }

  @Override
  public void close() {
    consumer.close();
  }
}
