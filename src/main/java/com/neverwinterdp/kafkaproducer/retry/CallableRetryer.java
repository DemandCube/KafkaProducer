package com.neverwinterdp.kafkaproducer.retry;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

public class CallableRetryer<T> implements Callable<T> {

  private static final Logger logger = Logger.getLogger(CallableRetryer.class);
  private static final AtomicInteger counter = new AtomicInteger(0);

  private RetryStrategy retryStrategy;
  private RetryableCallable<T> runnable;
  private boolean isSuccess;

  public CallableRetryer(RetryStrategy retryStrategy, RetryableCallable<T> runnable) {
    super();
    this.retryStrategy = retryStrategy;
    this.runnable = runnable;
    isSuccess = false;
  }

  @Override
  public T call() {
    retryStrategy.reset();
    T x;
    do {
      try {
        x = runnable.call();
        isSuccess = true;
        retryStrategy.shouldRetry(false);
        counter.incrementAndGet();
        return x;
      } catch (Exception ex) {
        logger.debug("We got an exception: " + ex.toString());
        retryStrategy.errorOccured(ex);
        if (retryStrategy.shouldRetry()) {
          try {
            runnable.beforeRetry();
            retryStrategy.await();
          } catch (InterruptedException e) {
            retryStrategy.shouldRetry(false);
          }
        } else {
          throw new RetryException("Runnable did not complete succesfully after "
              + retryStrategy.getRetries() + ". Last Exception was "
              + ex.getCause());
        }
      }
    } while (retryStrategy.shouldRetry());
    throw new RetryException();
  }

  public RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }

  public void setRetryStrategy(RetryStrategy retryStrategy) {
    this.retryStrategy = retryStrategy;
  }

  public RetryableCallable<T> getRunnable() {
    return runnable;
  }

  public void setRunnable(RetryableCallable<T> runnable) {
    this.runnable = runnable;
  }

  public boolean isSuccess() {
    return isSuccess;
  }

  public void setSuccess(boolean isSuccess) {
    this.isSuccess = isSuccess;
  }

  // hack
  public static AtomicInteger getCounter() {
    return counter;
  }

  // hack
  public static void resetCounter() {
    counter.set(0);
  }
}
