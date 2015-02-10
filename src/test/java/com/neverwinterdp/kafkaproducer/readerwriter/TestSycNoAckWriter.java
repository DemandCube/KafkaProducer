package com.neverwinterdp.kafkaproducer.readerwriter;

import java.util.Properties;

public class TestSycNoAckWriter extends AbstractReaderWriterTest {

  protected Properties initProperties() throws Exception {
    Properties props = new Properties();
    props.put("request.required.acks", "0");
    props.put("producer.type", "async");
    return props;
  }

}
