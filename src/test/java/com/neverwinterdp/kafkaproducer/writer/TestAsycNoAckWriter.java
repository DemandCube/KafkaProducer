package com.neverwinterdp.kafkaproducer.writer;

import java.util.Properties;

public class TestAsycNoAckWriter extends AbstractWriterTest {

  
  protected Properties initProperties() throws Exception{
    Properties props = new Properties();
    props.put("request.required.acks", "0");
    props.put("producer.type", "async");    
    return props;
  }
}
