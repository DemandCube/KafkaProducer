package com.neverwinterdp.kafka.servers;

public interface Server {

  String getHost();

  int getPort();

  void start() throws Exception;

  void shutdown();

}
