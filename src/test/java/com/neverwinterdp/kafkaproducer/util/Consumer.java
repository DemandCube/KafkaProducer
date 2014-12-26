package com.neverwinterdp.kafkaproducer.util;

import java.nio.ByteBuffer;
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

import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class Consumer implements AutoCloseable {

  private static final Logger logger = Logger.getLogger(Consumer.class);
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final int TIMEOUT = 10000;
  List<String> messages;
  private String zkURL;
  private ZookeeperHelper helper;
  private String topic;
  private int partition;
  private SimpleConsumer consumer;
  private boolean hasNextOffset;


  public Consumer(String zkURL, String topic, int partition) {
    super();
    this.zkURL = zkURL;
    this.topic = topic;
    this.partition = partition;
    messages = new LinkedList<String>();
    try {
      initialize();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }



  public void initialize() throws Exception {
    helper = new ZookeeperHelper(zkURL);
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, partition);

    consumer =
        new SimpleConsumer(leader.getHost(), leader.getPort(), TIMEOUT, BUFFER_SIZE,
            "test-consumer");

  }

  public List<String> read() {
    long currentOffset = getOffset(kafka.api.OffsetRequest.EarliestTime());
    do {
      FetchRequest req =
          new FetchRequestBuilder().clientId("test Consumer")
              .addFetch(topic, partition, currentOffset, 100000).build();

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
      } else {
        hasNextOffset = false;
      }
      currentOffset = nextOffset;
    } while (hasNextOffset);
    return messages;
  }

  /**
   * To get Earliest offset ask for kafka.api.OffsetRequest.EarliestTime(). To get latest offset ask
   * for kafka.api.OffsetRequest.LatestTime()
   */
  public long getOffset(long time) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    offsetInfo
        .put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(time, 1));
    System.out.println("cunsumer is null " + consumer == null);
    OffsetResponse response =
        consumer.getOffsetsBefore(new OffsetRequest(offsetInfo, kafka.api.OffsetRequest
            .CurrentVersion(), "test-consumer"));
    long[] endOffset = response.offsets(topic, partition);
    return endOffset[0];
  }



  @Override
  public void close() {
    consumer.close();
  }
}
