package com.neverwinterdp.kafkaproducer.retry;

public interface RetryableRunnable extends Runnable {

  void beforeRetry();
  void afterRetry();
  void beforeStart();
  void processFailed();
  int getFailureCount();
  void pause();
  void resume();
  void stop();

}
