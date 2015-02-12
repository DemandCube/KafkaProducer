package com.neverwinterdp.kafkaproducer.util;

import static com.google.common.base.Preconditions.checkArgument;

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

  public HostPort(String hostPort) {
    checkArgument(hostPort.contains(":"),
        "The provided connect String {} is not in the form host:port ", hostPort);
    String[] parts = hostPort.split(":");
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
