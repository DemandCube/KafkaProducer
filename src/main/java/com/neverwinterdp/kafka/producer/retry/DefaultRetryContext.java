package com.neverwinterdp.kafka.producer.retry;

import java.util.concurrent.TimeUnit;

// retry 5 mins, with constant wait
public class DefaultRetryContext implements RetryContext {

  private TimeUnit timeUnit;
  private int maxRetries = 0;
  private int retries;
  private int waitDuration;


  public DefaultRetryContext(int maxRetries, int waitDuration) {
    super();
    this.maxRetries = maxRetries;
    this.waitDuration = waitDuration;
    this.retries = 0;
    timeUnit = TimeUnit.SECONDS;
  }

  @Override
  public boolean shouldRetry() {
    return retries++ < maxRetries;
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
}
