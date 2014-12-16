package com.neverwinterdp.kafka.producer.retry;

import kafka.common.FailedToSendMessageException;
import kafka.producer.ProducerClosedException;

public abstract class RetryRunnable implements Runnable {

  private RetryContext retryContext;
  private Runnable runnable;

  public RetryRunnable(RetryContext retryContext, Runnable runnable) {
    super();
    this.retryContext = retryContext;
    this.runnable = runnable;
  }

  @Override
  public void run() {
    // TODO run, getError, wait, runAgain until maxRetries exhausted
    retryContext.reset();
    do {
      try {
        runnable.run();
        retryContext.setShouldRetry(false);
      } catch (ProducerClosedException | FailedToSendMessageException ex) {
        retryContext.setException(ex);
        retryContext.incrementRetryCount();
        try {
          retryContext.await();
        } catch (InterruptedException e) {
          retryContext.setShouldRetry(false);
        }
      }
    } while (retryContext.shouldRetry());
  }
}
