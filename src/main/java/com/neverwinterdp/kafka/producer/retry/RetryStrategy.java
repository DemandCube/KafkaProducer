package com.neverwinterdp.kafka.producer.retry;


public interface RetryStrategy {

  boolean shouldRetry();

  long getWaitDuration();

  void await(long waitDuration) throws InterruptedException;

  void reset();

  void incrementRetryCount();

  void shouldRetry(boolean shouldRetry);

  void errorOccured(Exception ex);

  int getRemainingTries();

  void setException(Exception nullPointerException);

  int getRetries();
}
