package com.neverwinterdp.kafkaproducer.readerwriter;

import java.util.Properties;

public class TestSycNoAckWriter extends AbstractReaderWriterTest {

  protected int row = 3;
  protected Properties initProperties() throws Exception {
    Properties props = new Properties();
    props.put("request.required.acks", "0");
    props.put("producer.type", "async");
    return props;
  }
  @Override
  protected int getRow() {
    // TODO Auto-generated method stub
    return 3;
  }

}
