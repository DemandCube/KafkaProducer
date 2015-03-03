package com.neverwinterdp.kafkaproducer.readerwriter;

import java.util.Properties;

public class TestSycWithAckWriter extends AbstractReaderWriterTest {

  @Override
  protected Properties initProperties() throws Exception {
    Properties props = new Properties();
    props.put("request.required.acks", "1");
    props.put("producer.type", "sync");
    return props;
  }

  //@Test(expected = FailedToSendMessageException.class)
  public void testWriteToNonExistentTopic() throws Exception {
    writeToNonExistentTopic();
  }

  @Override
  protected int getRow() {
    // TODO Auto-generated method stub
    return 4;
  }

}
