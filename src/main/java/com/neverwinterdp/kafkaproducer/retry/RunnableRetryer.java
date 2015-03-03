package com.neverwinterdp.kafkaproducer.retry;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

public class RunnableRetryer implements Runnable {

  private static final Logger logger = Logger.getLogger(RunnableRetryer.class);
  private static final AtomicInteger counter = new AtomicInteger(0);

  private RetryStrategy retryStrategy;
  private RetryableRunnable writer;
  private boolean isSuccess;
  private Thread watcher;

  public RunnableRetryer(RetryStrategy retryStrategy, RetryableRunnable runnable) {
    super();
    this.retryStrategy = retryStrategy;
    this.writer = runnable;
      watcher = new Thread() {
      public void run() {
        while (true) {
          int failureCount = writer.getFailureCount();
          
          if (failureCount > 0) {
            if (failureCount < 1000) {
              System.err.println("failureCount "+ failureCount);
              writer.pause();
              try {
                Thread.sleep(5000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              writer.processFailed();
              writer.resume();
            } else {
              writer.stop();
            }
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    };
    watcher.start();
    isSuccess = false;
  }

  @Override
  public void run() {
    retryStrategy.reset();
    do {
      try {
        writer.beforeStart();
        writer.run();
        isSuccess = true;
        retryStrategy.shouldRetry(false);
        counter.incrementAndGet();

      } catch (Exception ex) {
        logger.debug("We got an exception: " + ex.toString());
        retryStrategy.errorOccured(ex);
        if (retryStrategy.shouldRetry()) {
          try {
            writer.beforeRetry();
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
  
  public void stop(){
    watcher.stop();
  }

  public RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }

  public void setRetryStrategy(RetryStrategy retryStrategy) {
    this.retryStrategy = retryStrategy;
  }

  public RetryableRunnable getRunnable() {
    return writer;
  }

  public void setRunnable(RetryableRunnable runnable) {
    this.writer = runnable;
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
