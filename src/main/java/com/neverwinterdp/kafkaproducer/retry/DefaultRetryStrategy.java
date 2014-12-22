package com.neverwinterdp.kafkaproducer.retry;

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.isNull;
import static com.google.common.base.Predicates.or;

import com.google.common.base.Predicate;

// retry 5 times, with constant wait
public class DefaultRetryStrategy implements RetryStrategy {


  private int maxRetries = 0;
  private int retries;
  private int waitDuration;
  private Predicate<? super Exception> exceptionPredicate;
  private Exception exception;

  public DefaultRetryStrategy(int maxRetries, int waitDuration,
      Class<? extends Exception> retryableException) {
    super();
    this.maxRetries = maxRetries;
    this.waitDuration = waitDuration;
    this.retries = 0;
    this.exceptionPredicate = or(isNull(), instanceOf(retryableException));
  }

  @Override
  public boolean shouldRetry() {
    // retryCount less than max retries and exception is a retryable exception or is null
    return retries < maxRetries && (exceptionPredicate.apply(exception));
  }


  @Override
  public long getWaitDuration() {
    return this.waitDuration;
  }


  @Override
  public void reset() {
    retries = 0;
    exception = null;
  }


  @Override
  public void incrementRetryCount() {
    retries++;
  }

  @Override
  // TODO remove this method
  @Deprecated
  public void shouldRetry(boolean shouldRetry) {
    if (shouldRetry) {
      retries = maxRetries - 1;
    } else {
      retries = maxRetries + 1;
    }
  }

  @Override
  public void await(long waitDuration) throws InterruptedException {
    Thread.sleep(waitDuration);
  }

  @Override
  public void errorOccured(Exception ex) {
    this.exception = ex;
    incrementRetryCount();
  }

  @Override
  public int getRemainingTries() {
    return maxRetries - retries;
  }

  @Override
  public void setException(Exception exception) {
    this.exception = exception;

  }

  @Override
  public int getRetries() {
    return retries;
  }
}
