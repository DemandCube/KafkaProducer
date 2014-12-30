package com.neverwinterdp.kafkaproducer.retry;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.retry.DefaultRetryStrategy;
import com.neverwinterdp.kafkaproducer.retry.RetryException;
import com.neverwinterdp.kafkaproducer.retry.RunnableRetryer;
import com.neverwinterdp.kafkaproducer.retry.RetryStrategy;
import com.neverwinterdp.kafkaproducer.retry.RetryableRunnable;

public class TestRetryableRunnable {
 
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  ExecutorService executorService;
  RunnableRetryer retryRunnable;
  RetryStrategy retryStrategy;
  private int maxRetries = 5;
  private int waitDuration = 1000;

  @Before
  public void setUp() {
    // retry 5 times, every second in case of an NPE
    retryStrategy = new DefaultRetryStrategy(maxRetries, waitDuration, NullPointerException.class);
    executorService = Executors.newSingleThreadExecutor();
  }

  // throw NPE 5 times, we should still get a success
  // TODO get this test to pass
  @Test
  public void testOneFailure() {
    RetryableRunnable runnable = mock(RetryableRunnable.class);
    doThrow(new NullPointerException())
        .doNothing().when(runnable).run();


    RunnableRetryer retryRunnable = new RunnableRetryer(retryStrategy, runnable);
    Future<?> future = executorService.submit(retryRunnable);
    try {
      future.get();
    } catch (ExecutionException e) {
      fail("We should not get here.");
      assertThat(e.getCause(), not(instanceOf(RetryException.class)));
    } catch (InterruptedException e) {
    }
    assertTrue(retryRunnable.isSuccess());
  }

  @Test
  public void testMaxRetries() {
    RetryableRunnable runnable = mock(RetryableRunnable.class);
    // throw NPE 4 times, 5th time do nothing
    doThrow(new NullPointerException())
        .doThrow(new NullPointerException())
        .doThrow(new NullPointerException())
        .doThrow(new NullPointerException())
        .doNothing().when(runnable).run();


    RunnableRetryer retryRunnable = new RunnableRetryer(retryStrategy, runnable);
    Future<?> future = executorService.submit(retryRunnable);
    try {
      future.get();
    } catch (ExecutionException e) {
      fail("We should not get here.");
      assertThat(e.getCause(), not(instanceOf(RetryException.class)));
    } catch (InterruptedException e) {
    }
    assertTrue(retryRunnable.isSuccess());
  }

  // throw NPE 6 times. we should get a retry exception (after the 5th try)
  @Test
  public void testMaxRetriesPlusOne() {
    RetryableRunnable runnable = mock(RetryableRunnable.class);
    doThrow(new NullPointerException())
        .doThrow(new NullPointerException())
        .doThrow(new NullPointerException())
        .doThrow(new NullPointerException())
        .doThrow(new NullPointerException())
        .when(runnable).run();
    RunnableRetryer retryRunnable = new RunnableRetryer(retryStrategy, runnable);
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
    RetryableRunnable runnable = mock(RetryableRunnable.class);
    doThrow(new ArithmeticException()).when(runnable).run();


    RunnableRetryer retryRunnable = new RunnableRetryer(retryStrategy, runnable);
    Future<?> future = executorService.submit(retryRunnable);
    try {
      future.get();
      fail();
    } catch (ExecutionException | InterruptedException e) {
      assertThat(e.getCause(), instanceOf(RetryException.class));
    }
    assertFalse(retryRunnable.isSuccess());
  }

  // we should not retry when we get a parent exception
  @Test
  public void testParentException() {
    RetryableRunnable runnable = mock(RetryableRunnable.class);
    doThrow(new RuntimeException()).when(runnable).run();

    RunnableRetryer retryRunnable = new RunnableRetryer(retryStrategy, runnable);
    Future<?> future = executorService.submit(retryRunnable);
    try {
      future.get();
      assertFalse(retryRunnable.getRetryStrategy().shouldRetry());
    } catch (ExecutionException | InterruptedException e) {
      assertThat(e.getCause(), instanceOf(RetryException.class));
    }
    assertFalse(retryRunnable.isSuccess());
  }


  // but we should retry on a child Exception
  @Test
  public void testChildException() {
    RetryableRunnable runnable = mock(RetryableRunnable.class);
    doThrow(new ChildException()).doThrow(new ChildException()).doNothing().when(runnable).run();

    RunnableRetryer retryRunnable = new RunnableRetryer(retryStrategy, runnable);
    Future<?> future = executorService.submit(retryRunnable);
    try {
      future.get();
      assertFalse(retryRunnable.getRetryStrategy().shouldRetry());
    } catch (ExecutionException e) {
      // fail("We should never get here.");
      assertThat(e.getCause(), not(instanceOf(RetryException.class)));
    } catch (InterruptedException e) {
    }
    assertTrue(retryRunnable.isSuccess());
  }

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

  class ChildException extends NullPointerException {
    private static final long serialVersionUID = 1L;
  }
}
