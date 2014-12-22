package com.neverwinterdp.kafkaproducer.servers;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.BasicConfigurator;

import com.neverwinterdp.kafkaproducer.util.HostPort;

public class MyCluster {

  private int numOfZkInstances;
  private int numOfKafkaInstances;
  private Set<HostPort> zkHosts;
  private Set<HostPort> kafkaHosts;


  private HashMap<String, EmbeddedZookeeper> zookeeperServers;
  private HashMap<String, KafkaServer> kafkaServers;


  public MyCluster(int numOfZkInstances, int numOfKafkaInstances) {
    this.numOfZkInstances = numOfZkInstances;
    this.numOfKafkaInstances = numOfKafkaInstances;
    zookeeperServers = new HashMap<>();
    kafkaServers = new HashMap<>();

    zkHosts = new HashSet<>();
    kafkaHosts = new HashSet<>();
  }

  public void start() throws Exception {
    for (int i = 0; i < numOfZkInstances; i++) {
      // setup Zookeeper
      String zkConnect = TestZKUtils.zookeeperConnect();
      zkHosts.add(new HostPort(zkConnect));
      System.out.println(zkConnect);
      EmbeddedZookeeper zkServer = new EmbeddedZookeeper(zkConnect);
      ZkClient zkClient =
          new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
      zkClient.hashCode();
      zookeeperServers.put(zkConnect, zkServer);
    }


    for (int i = 0; i < numOfKafkaInstances; i++) {
      // setup Broker
      int port = TestUtils.choosePort();
      Properties props = TestUtils.createBrokerConfig(i, port, true);

      KafkaConfig config = new KafkaConfig(props);
      Time mock = new MockTime();
      KafkaServer kafkaServer = TestUtils.createServer(config, mock);
      kafkaHosts.add(new HostPort("127.0.0.1", port));
      kafkaServers.put(config.toString(), kafkaServer);
    }
  }

  public void shutdown() {
    for (KafkaServer server : kafkaServers.values()) {
      server.shutdown();
    }

    for (EmbeddedZookeeper server : zookeeperServers.values()) {
      server.shutdown();
    }
  }

  public static void main(String[] args) {
    BasicConfigurator.configure();
    MyCluster cluster = new MyCluster(1, 3);
    try {
      cluster.start();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Collection<KafkaServer> getKafkaServers() {
    return kafkaServers.values();
  }

  public Set<HostPort> getZkHosts() {
    return zkHosts;
  }

  public Set<HostPort> getKafkaHosts() {
    return kafkaHosts;
  }
}
