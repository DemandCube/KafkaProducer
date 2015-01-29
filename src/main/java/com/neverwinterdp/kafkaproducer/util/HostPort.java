package com.neverwinterdp.kafkaproducer.util;

public class HostPort {
  private String host;
  private int port;

  public HostPort(String host, String port) {
    this.host = host;
    this.port = Integer.parseInt(port);
  }

  public HostPort(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public HostPort(String zkConnect) {
    String[] parts = zkConnect.split(":");
    this.host = parts[0];
    this.port = Integer.parseInt(parts[1]);
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String toString() {
    return host + ":" + port;
  }
}
