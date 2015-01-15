package com.neverwinterdp.kafkaproducer.writer;


import java.util.Properties;


public class TestAsycWithAckWriter extends AbstractWriterTest {

  
  protected Properties initProperties() throws Exception{
    Properties props = new Properties();
    props.put("request.required.acks", "1");
    props.put("producer.type", "async");    
    return props;
  }
}
