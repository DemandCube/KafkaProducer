package com.neverwinterdp.kafka.producer.retry;

import java.util.concurrent.TimeUnit;

// retry 5 mins, with constant wait
public class DefaultRetryContext implements RetryContext {

  private TimeUnit timeUnit;
  private int maxRetries = 0;
  private int retries;
  private int waitDuration;
  private boolean shouldRetry;
  private Exception exception;


  public DefaultRetryContext(int maxRetries, int waitDuration) {
    super();
    this.maxRetries = maxRetries;
    this.waitDuration = waitDuration;
    this.retries = 0;
    this.shouldRetry = false;
    timeUnit = TimeUnit.SECONDS;
  }

  @Override
  public boolean shouldRetry() {
    // retryCount less than max retries and exception is a retryable exception
    shouldRetry = retries++ < maxRetries;
    return shouldRetry;
  }

  @Override
  public long getDelay() {
    return waitDuration;
  }

  @Override
  public TimeUnit getTimeUnit() {
    return this.timeUnit;
  }

  @Override
  public long waitDuration() {
    return this.waitDuration();
  }

  @Override
  public void await() throws InterruptedException {
    Thread.sleep(waitDuration);
  }

  @Override
  public void reset() {
    retries = 0;
    // exception = non

  }

  @Override
  public void setException(Exception e) {
    this.exception = e;
  }

  @Override
  public void incrementRetryCount() {
    ++retries;
  }

  @Override
  public void setShouldRetry(boolean shouldRetry) {
    this.shouldRetry = shouldRetry;
  }
}
