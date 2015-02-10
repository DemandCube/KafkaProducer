package com.neverwinterdp.kafkaproducer.retry;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

public class RunnableRetryer implements Runnable {

	private static final Logger logger = Logger
			.getLogger(RunnableRetryer.class);
	private static final AtomicInteger counter = new AtomicInteger(0);

	private RetryStrategy retryStrategy;
	private RetryableRunnable runnable;
	private boolean isSuccess;

	public RunnableRetryer(RetryStrategy retryStrategy,
			RetryableRunnable runnable) {
		super();
		this.retryStrategy = retryStrategy;
		this.runnable = runnable;
		isSuccess = false;
	}

	@Override
	public void run() {
		retryStrategy.reset();
		do {
			try {
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
					throw new RetryException(
							"Runnable did not complete succesfully after "
									+ retryStrategy.getRetries()
									+ " retries. Last Exception was "
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

	// hack
	public static AtomicInteger getCounter() {
		return counter;
	}

	// hack
	public static void resetCounter() {
		counter.set(0);
	}
}
