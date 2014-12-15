package com.neverwinterdp.kafka.producer.retry;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class RetryRunnable implements Runnable {

  ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  private RetryContext retryContext;
  private boolean success;
  private Runnable runnable;

  public RetryRunnable(RetryContext retryContext, Runnable runnable) {
    super();
    this.retryContext = retryContext;
    this.runnable = runnable;
  }

  void retry() {
    if (retryContext.shouldRetry())
      executorService.schedule(this, retryContext.getDelay(), retryContext.getTimeUnit());
  }

  @Override
  public void run() {
    try {
      runnable.run();
    } catch (Exception e) {
      if (e instanceof NullPointerException) {
        retry();
      }
    }
    // TODO run, getError, wait, runAgain until maxRetries exhausted
  }
}
