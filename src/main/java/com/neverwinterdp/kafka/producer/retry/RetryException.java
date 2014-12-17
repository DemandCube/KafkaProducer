package com.neverwinterdp.kafka.producer.retry;

public class RetryException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public RetryException() {
    super();
  }

  public RetryException(String message) {
    super(message);
  }
}
