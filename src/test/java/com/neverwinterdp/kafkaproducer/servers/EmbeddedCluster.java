package com.neverwinterdp.kafkaproducer.servers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;

import com.neverwinterdp.kafkaproducer.util.HostPort;

public class EmbeddedCluster {

  private int numOfZkInstances;
  private int numOfKafkaInstances;
  ZkClient zkClient;
  private Set<HostPort> zkHosts;
  private Set<HostPort> kafkaHosts;
  int brokerId = 0;
  private List<EmbeddedZookeeper> zookeeperServers;
  private List<KafkaServer> kafkaServers;

  public EmbeddedCluster(int numOfZkInstances, int numOfKafkaInstances) {
    this.numOfZkInstances = numOfZkInstances;
    this.numOfKafkaInstances = numOfKafkaInstances;

    zookeeperServers = new ArrayList<>();
    kafkaServers = new ArrayList<>();

    zkHosts = new HashSet<>();
    kafkaHosts = new HashSet<>();
  }

  public void start() throws Exception {
    for (int i = 0; i < numOfZkInstances; i++) {
      addZookeeperServer();
    }
    if (numOfZkInstances > 0)
      zkClient = new ZkClient(zookeeperServers.get(0).connectString(), 30000, 30000);

    for (int i = 0; i < numOfKafkaInstances; i++) {
      addKafkaServer();
    }
    System.out.println("Cluster created");
  }

  public void addZookeeperServer() {
    // setup Zookeeper
    String zkConnect = TestZKUtils.zookeeperConnect();
    zkHosts.add(new HostPort(zkConnect));
    EmbeddedZookeeper zkServer = new EmbeddedZookeeper(zkConnect);
    zookeeperServers.add(zkServer);
  }

  public void addKafkaServer() {
    // setup Broker
    int port = TestUtils.choosePort();
    Properties props = TestUtils.createBrokerConfig(brokerId++, port, true);

    KafkaConfig config = new KafkaConfig(props);
    Time mock = new MockTime();
    System.out.println("auto.leader.rebalance.enable: " + config.autoLeaderRebalanceEnable());
    System.out.println("controlled.shutdown.enabled " + config.controlledShutdownEnable());
    KafkaServer kafkaServer = TestUtils.createServer(config, mock);
    kafkaHosts.add(new HostPort("127.0.0.1", port));
    kafkaServers.add(kafkaServer);
    
    
  }

  public void startAdditionalBrokers(int numBrokers) {

    for (int i = 0; i < numBrokers; i++) {
      addKafkaServer();
    }

  }

  public void shutdown() {
    zkClient.close();
    for (KafkaServer server : kafkaServers) {
      server.shutdown();
    }

    for (EmbeddedZookeeper server : zookeeperServers) {
      server.shutdown();
    }
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      // ignore
    }
  }

  public List<KafkaServer> getKafkaServers() {
    return kafkaServers;
  }

  public Collection<EmbeddedZookeeper> getZookeeperServers() {
    return zookeeperServers;
  }

  public Set<HostPort> getZkHosts() {
    return zkHosts;
  }

  public Set<HostPort> getKafkaHosts() {
    return kafkaHosts;
  }

  // TODO get connectionString off all Zk's
  // 127.0.0.1:2181, 127.0.0.1:2182
  public String getZkURL() {
    return zookeeperServers.get(0).connectString();
  }

}
