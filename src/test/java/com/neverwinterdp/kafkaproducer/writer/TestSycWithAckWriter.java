package com.neverwinterdp.kafkaproducer.writer;


import java.util.Properties;


public class TestSycWithAckWriter extends TestWriter {

  
  protected Properties initProperties() throws Exception{
    Properties props = new Properties();
    props.put("request.required.acks", "1");
    props.put("producer.type", "async");    
    return props;
  }
  
  
}
