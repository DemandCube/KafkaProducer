package com.neverwinterdp.kafkaproducer.readerwriter;

import java.util.Properties;

public class TestAsycNoAckWriter extends AbstractReaderWriterTest {

  protected Properties initProperties() throws Exception {
    Properties props = new Properties();
    props.put("request.required.acks", "0");
    props.put("producer.type", "async");
    props.put("topic.metadata.refresh.interval.ms", 600 );
    props.put("retry.backoff.ms", 100 );
    props.put("message.send.max.retries", 10);
    
    return props;
  }
  @Override
  protected int getRow() {
    return 1;
  }

}
