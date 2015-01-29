package com.neverwinterdp.kafkaproducer.retry;

import java.util.concurrent.Callable;

public interface RetryableCallable<T> extends Callable<T> {

  void beforeRetry();

}
