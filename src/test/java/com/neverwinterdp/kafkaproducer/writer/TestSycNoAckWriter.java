package com.neverwinterdp.kafkaproducer.writer;

import java.util.Properties;

import kafka.common.FailedToSendMessageException;

import org.junit.Test;

import com.neverwinterdp.kafkaproducer.util.TestUtils;

public class TestSycNoAckWriter extends AbstractWriterTest {

  protected Properties initProperties() throws Exception {
    Properties props = new Properties();
    props.put("request.required.acks", "0");
    props.put("producer.type", "async");
    return props;
  }
  
  @Test(expected=FailedToSendMessageException.class)
  public void testWriteToNonExistentTopic() throws Exception {
      writeToNonExistentTopic();
  }

}
