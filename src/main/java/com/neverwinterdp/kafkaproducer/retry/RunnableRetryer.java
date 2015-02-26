package com.neverwinterdp.kafkaproducer.retry;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

public class RunnableRetryer implements Runnable {

  private static final Logger logger = Logger.getLogger(RunnableRetryer.class);
  private static final AtomicInteger counter = new AtomicInteger(0);

  private RetryStrategy retryStrategy;
  private RetryableRunnable runnable;
  private boolean isSuccess;

  public RunnableRetryer(RetryStrategy retryStrategy, RetryableRunnable rble) {
    super();
    this.retryStrategy = retryStrategy;
    this.runnable = rble;
    new Thread() {
      public void run() {
        while (true) {
          int failureCount = runnable.getFailureCount();
          System.err.println("failureCount "+ failureCount);
          if (failureCount > 0) {
            if (failureCount < 1000) {
              runnable.pause();
              try {
                Thread.sleep(5000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              runnable.processFailed();
              runnable.resume();
            } else {
              runnable.stop();
            }
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }.start();
    isSuccess = false;
  }

  @Override
  public void run() {
    retryStrategy.reset();
    do {
      try {
        runnable.beforeStart();
        runnable.run();
        isSuccess = true;
        retryStrategy.shouldRetry(false);
        counter.incrementAndGet();

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
          throw new RetryException("Runnable did not complete succesfully after " + retryStrategy.getRetries()
              + ". Last Exception was " + ex.getCause());
        }
      }
    } while (retryStrategy.shouldRetry());
  }

  public RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }

  public void setRetryStrategy(RetryStrategy retryStrategy) {
    this.retryStrategy = retryStrategy;
  }

  public RetryableRunnable getRunnable() {
    return runnable;
  }

  public void setRunnable(RetryableRunnable runnable) {
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
