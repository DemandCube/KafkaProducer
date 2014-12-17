package com.neverwinterdp.kafka.producer.retry;


public class MyRunnable implements RetryableRunnable {

  private int maxRetries;
  int retries = 0;
  private RuntimeException exception;

  public MyRunnable(int i) {
    this.maxRetries = i;
    exception = new NullPointerException();
  }

  public MyRunnable(int i, RuntimeException illegalArgumentException) {
    this.maxRetries = i;
    this.exception = illegalArgumentException;
  }

  @Override
  public void run() {
    if (retries <= maxRetries)
      throw exception;
    retries++;
  }

  @Override
  public void beforeRetry() {
    System.out.println("before retry");
  }

  @Override
  public void afterRetry() {
    System.out.println("after retry");
  }
}
