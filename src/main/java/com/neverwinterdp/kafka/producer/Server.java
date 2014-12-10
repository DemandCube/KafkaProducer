package com.neverwinterdp.kafka.producer;

public interface Server {

  String getHost();

  int getPort();

  void start();
  
  void shutdown();

}
