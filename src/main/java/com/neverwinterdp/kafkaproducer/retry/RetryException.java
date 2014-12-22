package com.neverwinterdp.kafkaproducer.retry;

public class RetryException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public RetryException() {
    super();
  }

  public RetryException(String message) {
    super(message);
  }
}
