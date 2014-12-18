package com.neverwinterdp.kafka.producer.retry;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class TestDefaultRetryStrategy {

  private RetryStrategy retryStrategy;
  private int maxRetries = 5;
  private int waitDuration = 1000;

  @Before
  public void setUp() {
    // create a retryStrategy to retry on null pointer exception
    retryStrategy = new DefaultRetryStrategy(maxRetries, waitDuration, NullPointerException.class);
  }

  // retries if we get a NPE
  @Test
  public void testRetryOnException() {
    retryStrategy.setException(new NullPointerException());
    assertTrue(retryStrategy.shouldRetry());
  }

  // does not retry since we throw an IAE
  @Test
  public void testRetryBadException() {
    retryStrategy.setException(new IllegalArgumentException());
    assertFalse(retryStrategy.shouldRetry());
  }


  // should retry on null exception
  @Test
  public void testRetryNullException() {
    retryStrategy.setException(null);
    assertTrue(retryStrategy.shouldRetry());
  }

  // should retry 5 times
  @Test
  public void testRetryMaxTimes() {
    for (int i = 1; i <= maxRetries; i++) {
      assertTrue(retryStrategy.shouldRetry());
      retryStrategy.errorOccured(new NullPointerException());
    }
    assertFalse(retryStrategy.shouldRetry());
  }

  @Test
  public void testRetryOnSuccess() {
    retryStrategy.shouldRetry(true);
    assertTrue(retryStrategy.shouldRetry());

    retryStrategy.shouldRetry(false);
    assertFalse(retryStrategy.shouldRetry());
  }

  @Test
  public void testForcedRetry() {
    retryStrategy.shouldRetry(true);
    assertTrue(retryStrategy.shouldRetry());

    for (int i = 0; i < maxRetries * 2; i++) {
      retryStrategy.errorOccured(new NullPointerException());
    }
    
    retryStrategy.shouldRetry(true);
    assertTrue(retryStrategy.shouldRetry());
  }

  @Test
  public void testAwait() throws InterruptedException {
    long now = System.currentTimeMillis();
    retryStrategy.await(waitDuration);
    long after = System.currentTimeMillis();

    assertTrue(after >= now + waitDuration);
  }

  @Test
  public void testReset() throws InterruptedException {
    for (int i = 0; i < maxRetries * 2; i++) {
      retryStrategy.errorOccured(new NullPointerException());
    }
    assertFalse(retryStrategy.shouldRetry());
    retryStrategy.reset();
    assertTrue(retryStrategy.shouldRetry());
  }

  // 3 runs throw a retryable exception, 4th run is a non-retryable exception
  @Test
  public void testRetryVariousExceptions() throws InterruptedException {
    for (int i = 0; i < 3; i++) {
      retryStrategy.errorOccured(new NullPointerException());
    }
    retryStrategy.errorOccured(new IllegalArgumentException());
    assertFalse(retryStrategy.shouldRetry());
  }

  @Test
  public void testRetryExceptions() throws InterruptedException {
    retryStrategy.errorOccured(new Exception());
    assertFalse(retryStrategy.shouldRetry());

    retryStrategy.errorOccured(new RuntimeException());
    assertFalse(retryStrategy.shouldRetry());
  }

  // a child exception is thrown, we should retry
  @Test
  public void testChildExceptions() throws InterruptedException {
    retryStrategy.errorOccured(new ChildException());
    assertTrue(retryStrategy.shouldRetry());
  }

  class ChildException extends NullPointerException {
    private static final long serialVersionUID = 1L;
  }
}
