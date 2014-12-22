package com.neverwinterdp.kafkaproducer.retry;

public interface RetryableRunnable extends Runnable {

  void beforeRetry();
  
  void afterRetry();

}
