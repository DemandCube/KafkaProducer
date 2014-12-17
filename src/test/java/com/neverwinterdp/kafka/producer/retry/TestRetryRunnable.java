package com.neverwinterdp.kafka.producer.retry;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestRetryRunnable {

  ExecutorService executorService;
  RetryRunnable retryRunnable;
  RetryStrategy retryStrategy;
  private int maxRetries = 5;
  private int waitDuration = 1000;

  @Before
  public void setUp() {
    BasicConfigurator.configure();
    // retry 5 times, every second in case of an NPE
    retryStrategy = new DefaultRetryStrategy(maxRetries, waitDuration, NullPointerException.class);
    executorService = Executors.newSingleThreadExecutor();
  }

  // throw NPE 5 times, we should still get a success
  // TODO get this test to pass
  @Test
  @Ignore
  public void testMaxRetries() {
    RetryRunnable retryRunnable = new RetryRunnable(retryStrategy, new MyRunnable(maxRetries));
    Future<?> future = executorService.submit(retryRunnable);
    try {
      future.get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), not(instanceOf(RetryException.class)));
    } catch (InterruptedException e) {
    }
    assertTrue(retryRunnable.isSuccess());
  }

  // throw NPE 6 times. we should get a retry exception (after the 5th try)
  @Test
  public void testMaxRetriesPlusOne() {
    RetryRunnable retryRunnable = new RetryRunnable(retryStrategy, new MyRunnable(maxRetries + 1));
    Future<?> future = executorService.submit(retryRunnable);
    try {
      future.get();
    } catch (ExecutionException | InterruptedException e) {
      assertThat(e.getCause(), instanceOf(RetryException.class));
    }
    assertFalse(retryRunnable.isSuccess());
  }

  // we throw a non-retriable exception
  @Test
  public void testNonRetryableException() {
    System.out.println("the part that matters");
    RetryRunnable retryRunnable =
        new RetryRunnable(retryStrategy, new MyRunnable(1, new IllegalArgumentException()));
    Future<?> future = executorService.submit(retryRunnable);
    try {
      future.get();
    } catch (ExecutionException | InterruptedException e) {
      assertThat(e.getCause(), instanceOf(RetryException.class));
    }
    assertFalse(retryRunnable.isSuccess());
    System.out.println("matters no more");
  }

  // we should not retry when we get a parent exception
  @Test
  @Ignore
  public void testParentException() {}

  @Test
  @Ignore
  public void testChildException() {}

  // we should not retry on success;
  @Test
  public void testRetryOnSuccess() {}

  @After
  @Ignore
  public void tearDown() throws InterruptedException {
    executorService.awaitTermination((waitDuration * maxRetries) + waitDuration,
        TimeUnit.MILLISECONDS);
    executorService.shutdown();
  }
}
