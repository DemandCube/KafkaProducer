package com.neverwinterdp.kafka.producer.servers;

import java.util.HashMap;
import java.util.Map;

public class TuanKafkaCluster {
  private int                 baseZKPort          = 2181;
  private int                 baseKafkaPort       = 9092;
  private int                 numOfKafkaInstances = 3;
  private int                 numOfZkInstances    = 1;
  private String              serverDir;
  private Map<String, Server> kafkaServers;
  private Map<String, Server> zookeeperServers;
  
  public TuanKafkaCluster(String serverDir) {
    this(serverDir, 1, 3);
  }
 
  
  public TuanKafkaCluster(String serverDir, int numOfZkInstances, int numOfKafkaInstances) {
    this.serverDir = serverDir;
    this.numOfKafkaInstances = numOfZkInstances;
    this.numOfKafkaInstances = numOfKafkaInstances;
    zookeeperServers = new HashMap<String, Server>();
    kafkaServers    = new HashMap<String, Server>();
  }
  
  public TuanKafkaCluster setBaseZKPort(int port) {
    this.baseZKPort = port;
    return this;
  }
  
  public TuanKafkaCluster setBaseKafkaPort(int port) {
    this.baseKafkaPort = port;
    return this;
  }

  public void start() throws Exception {
    for(int i = 0; i < numOfZkInstances; i++) {
      String serverName = "zookeeper-" + (i + 1);
      ZookeeperServerLauncher zookeeper = new ZookeeperServerLauncher(serverDir+"/" + serverName, baseZKPort + i);
      zookeeper.start();
      zookeeperServers.put(serverName, zookeeper);
    }

    for (int i = 0; i < numOfKafkaInstances; i++) {
      int replication = 1;
      if(numOfKafkaInstances > 1) replication = 2;
      int id = i + 1;
      String serverName = "kafka-" + id;
      KafkaServerLauncher kafka = 
          new KafkaServerLauncher(id, serverDir+"/" + serverName, baseKafkaPort + i, replication);
      kafka.start();
      kafkaServers.put(serverName, kafka);
    }
  }


  public Map<String, Server> getKafkaServerMap() { return kafkaServers; }

  public Server[] getKafkaServers() {
    Server[] server = new Server[kafkaServers.size()] ;
    kafkaServers.values().toArray(server);
    return server;
  }
  
  public Map<String, Server> getzookeeperServers() { return zookeeperServers; }

  public void shutdown() throws Exception {
    for (Server server : kafkaServers.values()) {
      server.shutdown();
    }

    for (Server server : zookeeperServers.values()) {
      server.shutdown();
    }
  }
  
  public String getZKConnect() {
    StringBuilder b = new StringBuilder();
    for (Server server : zookeeperServers.values()) {
      if(b.length() > 0) b.append(",");
      b.append("127.0.0.1:").append(server.getPort());
    }
    return b.toString();
  }
  
  public String getKafkaConnect() {
    StringBuilder b = new StringBuilder();
    for (Server server : kafkaServers.values()) {
      if(b.length() > 0) b.append(",");
      b.append("127.0.0.1:").append(server.getPort());
    }
    return b.toString();
  }
}
