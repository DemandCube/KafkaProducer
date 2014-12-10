package com.neverwinterdp.kafka.producer;

import java.util.Set;


public class TestServers {
  
  private Set<Server> kafkaServers;
  private Set<Server> zookeeperServers;

  public void start() throws Exception {}


  public Set<Server> getKafkaServers() {
    return kafkaServers;
  }

  public Set<Server> getzookeeperServers() {
    return zookeeperServers;
  }

  public void shutdown() {
    for (Server server : kafkaServers) {
      server.shutdown();
    }

    for (Server server : zookeeperServers) {
      server.shutdown();
    }
  }
}
