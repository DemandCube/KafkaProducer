package com.neverwinterdp.kafka.consumer;

import java.io.Closeable;

public class KafkaReader implements Closeable {

  public KafkaReader(String zkURL, String topic, int partition, int id) {
    // TODO Auto-generated constructor stub
  }

  public boolean hasNext() {
    // TODO Auto-generated method stub
    return false;
  }

  public String read() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }
}
