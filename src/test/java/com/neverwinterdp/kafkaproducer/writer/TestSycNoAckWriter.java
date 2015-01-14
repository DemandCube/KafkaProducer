package com.neverwinterdp.kafkaproducer.writer;

import java.util.Properties;

import kafka.common.FailedToSendMessageException;

import org.junit.Test;

import com.neverwinterdp.kafkaproducer.util.TestUtils;

public class TestSycNoAckWriter extends TestWriter {

  protected Properties initProperties() throws Exception {
    Properties props = new Properties();
    props.put("request.required.acks", "0");
    props.put("producer.type", "sync");
    return props;
  }
  @Test(expected = FailedToSendMessageException.class)
  public void testWriteToNonExistentTopic() throws Exception {
    try {
      initCluster(1, 1);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(zkURL, "someTopic").properties(props).partition(99)
          .build();
      writer.write("my message");
    } finally {
      cluster.shutdown();
    }
  }
}
