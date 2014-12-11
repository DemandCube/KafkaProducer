package com.neverwinterdp.kafka.consumer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;

import com.neverwinterdp.kafka.producer.util.HostPort;
import com.neverwinterdp.kafka.producer.util.ZookeeperHelper;

// TODO die if asked to read a non existent topic/partition
public class KafkaReader implements Closeable {
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final int TIMEOUT = 10000;
  private static final Logger logger = Logger.getLogger(KafkaReader.class);
  private ZookeeperHelper helper;

  private long currentOffset = 0;
  private SimpleConsumer consumer;
  private String zkURL;
  private String topic;
  private int partition;
  private boolean firstRun;
  private boolean hasNextOffset = true;

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
    firstRun = true;
  }

  // One offset many messages?
  public List<String> read() {
    if (firstRun) {
      currentOffset = getOffset(kafka.api.OffsetRequest.EarliestTime());
    }
    firstRun = false;
    List<String> messages = new LinkedList<>();
    FetchRequest req = new FetchRequestBuilder()
        .clientId(getClientName())
        .addFetch(topic, partition, currentOffset, 100000)
        .build();

    FetchResponse resp = consumer.fetch(req);
    if (resp.hasError()) {
      System.out.println("Error! " + resp.errorCode(topic, partition));
    }
    byte[] bytes = null;
    long nextOffset = currentOffset;
    for (MessageAndOffset messageAndOffset : resp.messageSet(topic, partition)) {
      long messageOffset = messageAndOffset.offset();
      if (messageOffset < currentOffset) {
        System.out
            .println("Found an old offset: " + messageOffset + " Expecting: " + currentOffset);
        continue;
      }

      ByteBuffer payload = messageAndOffset.message().payload();
      bytes = new byte[payload.limit()];
      payload.get(bytes);
      messages.add(new String(bytes));
      logger.info("current offset " + currentOffset + " " + messageAndOffset.offset() + ": "
          + new String(bytes));
      nextOffset = messageAndOffset.nextOffset();
    }
    logger.info("currentOffset:" + currentOffset + " nextOffset:" + nextOffset);
    if (currentOffset < nextOffset) {
      hasNextOffset = true;
    }
    else {
      hasNextOffset = false;
    }
    currentOffset = nextOffset;
    return messages;
  }

  /**
   * Return true if for the topic/partition we have an offset > currentOffset
   * */
  public boolean hasNext() {
    return hasNextOffset || currentOffset < getOffset(kafka.api.OffsetRequest.LatestTime());
  }

  /**
   * To get Earliest offset ask for kafka.api.OffsetRequest.EarliestTime().
   * To get latest offset ask for kafka.api.OffsetRequest.LatestTime()
   * */
  public long getOffset(long time) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    offsetInfo.put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(
        time, 1));
    OffsetResponse response = consumer
        .getOffsetsBefore(new OffsetRequest(offsetInfo, kafka.api.OffsetRequest
            .CurrentVersion(), getClientName()));
    long[] endOffset = response.offsets(topic, partition);
    logger.info("endoffsets:" + Arrays.toString(endOffset) + " TIME:" + time);
    return endOffset[0];
  }

  private String getClientName() {
    return topic + "_" + partition;
  }

  @Override
  public void close() {
    consumer.close();
  }
}
