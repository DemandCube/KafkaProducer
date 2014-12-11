package com.neverwinterdp.kafka.producer.servers;

public interface Server {

  String getHost();

  int getPort();

  void start() throws Exception;
  
  void shutdown();

}
