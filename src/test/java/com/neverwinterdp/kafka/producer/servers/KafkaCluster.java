package com.neverwinterdp.kafka.producer.servers;

import java.util.HashSet;
import java.util.Set;

public class KafkaCluster {

  private Set<Server> kafkaServers;
  private Set<Server> zookeeperServers;
  private int zkPort;
  private int kafkaPort;
  private int kafkaBrokers;
  private String dataDir;

  public KafkaCluster(String dataDir, int zkPort, int kafkaPort, int kafkaBrokers) {
    super();
    this.dataDir = dataDir;
    this.zkPort = zkPort;
    this.kafkaPort = kafkaPort;
    this.kafkaBrokers = kafkaBrokers;
    zookeeperServers = new HashSet<>();
    kafkaServers= new HashSet<>(kafkaBrokers);
  }


  public void start() throws Exception {
    ZookeeperServerLauncher zookeeper = new ZookeeperServerLauncher(dataDir+"/zk", zkPort);
    zookeeper.start();
    zookeeperServers.add(zookeeper);
    Thread.sleep(1000);
    KafkaServerLauncher kafka;
    for (int i = 0; i < kafkaBrokers; i++) {
      kafka = new KafkaServerLauncher(i, dataDir+"/kafka"+i, kafkaPort++, 1);
      kafka.start();
      kafkaServers.add(kafka);
      Thread.sleep(1000);
    }    
  }


  public Set<Server> getKafkaServers() {
    return kafkaServers;
  }

  public Set<Server> getzookeeperServers() {
    return zookeeperServers;
  }

  public void shutdown() throws Exception {
    for (Server server : kafkaServers) {
      server.shutdown();
    }

    for (Server server : zookeeperServers) {
      server.shutdown();
    }
  }
}
