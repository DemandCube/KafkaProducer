package com.neverwinterdp.kafka.producer.retry;

//TODO rename
public class RetryRunnable implements Runnable {

  private RetryStrategy retryStrategy;
  private RetryableRunnable runnable;
  private boolean isSuccess;

  public RetryRunnable(RetryStrategy retryStrategy, RetryableRunnable runnable) {
    super();
    this.retryStrategy = retryStrategy;
    this.runnable = runnable;
    isSuccess = false;
  }

  @Override
  public void run() {
    // TODO run, getError, wait, runAgain until maxRetries exhausted
    // throw retry exception if we retried many times but didn't succeed
    retryStrategy.reset();
    do {
      try {
        runnable.run();
        isSuccess = true;
        retryStrategy.shouldRetry(false);
      } catch (Exception ex) {
        retryStrategy.errorOccured(ex);
        if (retryStrategy.shouldRetry()) {
          try {
            runnable.beforeRetry();
            retryStrategy.await(retryStrategy.getWaitDuration());
          } catch (InterruptedException e) {
            retryStrategy.shouldRetry(false);
          }
        } else {
          // TODO proper message to user
          throw new RetryException("retries " + retryStrategy.getRemainingTries() + " reason "
              + ex.getCause());
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
}
