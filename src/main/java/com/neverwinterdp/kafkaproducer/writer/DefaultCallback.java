package com.neverwinterdp.kafkaproducer.writer;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * org.apache.kafka.common.errors.TimeoutException
 * */
public class DefaultCallback implements Callback {

  private static AtomicInteger integer = new AtomicInteger(0);

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    System.out.println("HAHAHA " + integer.incrementAndGet() + " " + exception);
  }

  public int getIntValue() {
    return integer.intValue();
  }
}
