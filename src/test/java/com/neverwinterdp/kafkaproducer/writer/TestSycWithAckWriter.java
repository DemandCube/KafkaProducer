package com.neverwinterdp.kafkaproducer.writer;

import java.util.Properties;

import kafka.common.FailedToSendMessageException;

import org.junit.Test;

public class TestSycWithAckWriter extends AbstractWriterTest {

  @Override
  protected Properties initProperties() throws Exception {
    Properties props = new Properties();
    props.put("request.required.acks", "1");
    props.put("producer.type", "sync");
    return props;
  }

  @Test(expected = FailedToSendMessageException.class)
  public void testWriteToNonExistentTopic() throws Exception {
    writeToNonExistentTopic();
  }

}
