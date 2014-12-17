package com.neverwinterdp.kafka.producer.retry;

public interface RetryableRunnable extends Runnable {

  void beforeRetry();
  
  void afterRetry();

}
