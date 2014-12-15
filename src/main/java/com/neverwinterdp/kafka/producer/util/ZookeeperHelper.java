package com.neverwinterdp.kafka.producer.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

// TODO get the names right
// TODO tests
// TODO soround in retry block
public class ZookeeperHelper implements Closeable {

  private String zkConnectString;
  private final static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

  private CuratorFramework zkClient;
  private PathChildrenCache pathChildrenCache;

  private static final Logger logger = Logger
      .getLogger(ZookeeperHelper.class);
  private String brokerInfoLocation = "/brokers/ids/";
  private String topicInfoLocation = "/brokers/topics/";

  public ZookeeperHelper(String zookeeperURL) throws InterruptedException {
    super();
    zkConnectString = zookeeperURL;
    zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        retryPolicy);
    init();
  }

  private void init() throws InterruptedException {
    zkClient.start();
    zkClient.blockUntilConnected();
  }

  public HostPort getLeaderForTopicAndPartition(String topic, int partition) throws Exception {

    String[] values = getLeader(topic, partition).split(
        ":");
    if (values.length == 2)
      return new HostPort(values[0], values[1]);
    else
      return null;
  }

  private String getLeader(String topic, int partition)
      throws Exception {
    String leader = "";

    PartitionState partitionState = getPartitionState(topic, partition);
    int leaderId = partitionState.getLeader();
    byte[] bytes = {};

    try {
      if (leaderId == -1) {
        return leader;
      }
      logger.debug("Going to look for " + brokerInfoLocation + leaderId);
      bytes = zkClient.getData().forPath(brokerInfoLocation + leaderId);
    }

    catch (NoNodeException nne) {
      logger.error(nne.getMessage(), nne);
      return leader;
    }
    Broker part = Utils.toClass(bytes, Broker.class);
    logger.debug("leader " + part);

    return leader.concat(part.getHost()).concat(":")
        .concat(String.valueOf(part.getPort()));

  }

  /* /brokers/[0...N] --> { "host" : "host:port",
                            "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
                                        "topicN": ["partition1" ... "partitionN"] } }*/
  public Collection<HostPort> getBrokersForTopicAndPartition(String topic, int partition)
      throws Exception {
    PartitionState partitionState = getPartitionState(topic, partition);
    Collection<HostPort> brokers = new LinkedList<>();
    HostPort broker;
    byte[] partitions;
    logger.debug("PartitionState " + partitionState);

    for (Integer b : partitionState.getIsr()) {
      // TODO broker is registered but offline next line throws
      // nonodeexception
      try {
        partitions = zkClient.getData().forPath(brokerInfoLocation + b);
        Broker part = Utils.toClass(partitions, Broker.class);
        broker = new HostPort(part.getHost(), part.getPort());
        brokers.add(broker);
      } catch (NoNodeException nne) {
        logger.debug(nne.getMessage());
      }
    }
    return brokers;
  }

  public Map<String, String> getData(String path) throws Exception {
    System.err.println("getData. path: " + path);
    if (zkClient.checkExists().forPath(path) == null) {
      return Collections.emptyMap();
    }
    byte[] data = zkClient.getData().forPath(path);
    return Utils.toMap(data);
  }

  public boolean checkPathExists(String config) throws Exception {
    return zkClient.checkExists().forPath(config) != null;
  }

  public int writeData(String path, byte[] data) throws Exception {
    //TODO exit if data is not a json obj
    logger.info("writeData. path: " + path + " data: " + Arrays.toString(data));
    if (zkClient.checkExists().forPath(path) == null) {

      String created =
          zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
              .forPath(path);
      logger.debug("what happened " + created);
    }
    Stat stat = zkClient.setData().forPath(path, data);

    return stat.getDataLength();
  }

  public boolean deletePath(String path) {
    try {
      zkClient.delete().deletingChildrenIfNeeded().forPath(path);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  public Multimap<Integer, HostPort> getBrokersForTopic(String topic) throws Exception {
    logger.info("getBrokersForTopic. ");
    Multimap<Integer, HostPort> brokers = HashMultimap.create();
    Topic topik = getTopicInfo(topic);
    HostPort hostPort;
    for (Entry<String, Set<Integer>> topicPartition : topik.partitions.entrySet()) {
      for (Integer replicaID : topicPartition.getValue()) {
        byte[] partitions = zkClient.getData().forPath(brokerInfoLocation + replicaID);
        Broker part = Utils.toClass(partitions, Broker.class);
        hostPort = new HostPort(part.getHost(), part.getPort());
        brokers.put(Integer.parseInt(topicPartition.getKey()), hostPort);
      }
    }
    logger.info("Broker.size " + brokers.size());
    return brokers;
  }

  /**
   * @param topic
   * @param partion
   * @return
   * @throws Exception
   */
  private PartitionState getPartitionState(String topic, int partion)
      throws Exception {
    try {
      zkClient.getData().forPath(topicInfoLocation + topic);
    } catch (NoNodeException nne) {
      // there are no nodes for the topic. We return an empty
      // Partitionstate
      logger.error(nne.getMessage());
      return new PartitionState();
    }
    byte[] bytes = zkClient.getData()
        .forPath(
            topicInfoLocation + topic + "/partitions/" + partion
                + "/state");
    PartitionState partitionState = Utils.toClass(bytes,
        PartitionState.class);
    return partitionState;
  }

  /**
  * @param topic
  * @param partion
  * @return
  * @throws Exception
  */
  private Topic getTopicInfo(String topic)
      throws Exception {
    try {
      zkClient.getData().forPath(topicInfoLocation + topic);
    } catch (NoNodeException nne) {
      // there are no nodes for the topic. We return an empty
      // Partitionstate
      logger.error(nne.getMessage());
      return new Topic();
    }
    byte[] bytes = zkClient.getData()
        .forPath(
            topicInfoLocation + topic);
    Topic topik = Utils.toClass(bytes,
        Topic.class);
    return topik;
  }

  // where is the info
  // brokers/id and /brokers/topics
  // to be used only when kafka broker info is not stored in default zookeeper
  // location
  public void setBrokerInfoLocation(String brokerInfoLocation) {
    this.brokerInfoLocation = brokerInfoLocation;
  }

  public void setTopicInfoLocation(String topicInfoLocation) {
    this.topicInfoLocation = topicInfoLocation;
  }

  //write to 
  public void updateProgress(String path, byte[] data) throws Exception {
    writeData(path, data);
  }

  public void deleteKafkaData() {
    logger.info("removeKafkaData. ");
    try {
      zkClient.delete().deletingChildrenIfNeeded().forPath("/brokers");
    } catch (Exception e) {
      e.printStackTrace();
    }
    logger.info("deleted brokers");
    try {
      zkClient.delete().deletingChildrenIfNeeded().forPath("/controller");
    } catch (Exception e) {
    }

    try {
      zkClient.delete().deletingChildrenIfNeeded().forPath("/controller_epoch");
    } catch (Exception e) {
    }
    try {
      zkClient.delete().deletingChildrenIfNeeded().forPath("/admin");
    } catch (Exception e) {
    }
    try {
      zkClient.delete().deletingChildrenIfNeeded().forPath("/config");
    } catch (Exception e) {
    }
  }

  /*  //Listener for node changes
    public void setTopicNodeListener(TopicNodeListener topicNodeListener) throws Exception {
      // in this example we will cache data. Notice that this is optional.
      logger.info("setTopicNodeListener. ");
      pathChildrenCache =
          new PathChildrenCache(zkClient, topicInfoLocation + topicNodeListener.getTopic(),
              true);
      pathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE);

      pathChildrenCache.getListenable().addListener(topicNodeListener);
    }*/

  @Override
  public void close() throws IOException {
    if (pathChildrenCache != null)
      pathChildrenCache.close();
    zkClient.close();
  }

  public void addPartitions(String topic, int partitions) {
    ZkClient client = new ZkClient(zkConnectString, 10000, 10000, ZKStringSerializer$.MODULE$);
    AdminUtils.addPartitions(client, topic, partitions, "");
    client.close();
  }

  public void createTopic(String topic, int partitions, int replicationFactor) {
    ZkClient client = new ZkClient(zkConnectString, 10000, 10000, ZKStringSerializer$.MODULE$);
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(client, topic, partitions, replicationFactor, topicConfig);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
    //Hack
    }
    client.close();
  }

  public void deleteTopic(String topic) {
    ZkClient client = new ZkClient(zkConnectString, 10000);
    client.deleteRecursive(ZkUtils.getTopicPath(topic));
    client.close();
  }
}


class Broker {
  String host;
  int jmx_port;
  int port;
  String timestamp;
  int version;

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getJmx_port() {
    return jmx_port;
  }

  public void setJmx_port(int jmx_port) {
    this.jmx_port = jmx_port;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "Partition [host=" + host + ", jmx_port=" + jmx_port + ", port="
        + port + ", timestamp=" + timestamp + ", version=" + version
        + "]";
  }
}


class PartitionState {
  int controller_epoch;
  Set<Integer> isr;
  int leader;
  int leader_epoch;
  int version;

  public PartitionState() {
    super();
    isr = Collections.emptySet();
  }

  public int getController_epoch() {
    return controller_epoch;
  }

  public void setController_epoch(int controller_epoch) {
    this.controller_epoch = controller_epoch;
  }

  public Set<Integer> getIsr() {
    return isr;
  }

  public void setIsr(Set<Integer> isr) {
    this.isr = isr;
  }

  public int getLeader() {
    return leader;
  }

  public void setLeader(int leader) {
    this.leader = leader;
  }

  public int getLeader_epoch() {
    return leader_epoch;
  }

  public void setLeader_epoch(int leader_epoch) {
    this.leader_epoch = leader_epoch;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "PartitionState [controller_epoch=" + controller_epoch
        + ", isr=" + isr + ", leader=" + leader + ", leader_epoch="
        + leader_epoch + ", version=" + version + "]";
  }
}


class Topic {
  int version;
  Map<String, Set<Integer>> partitions;


  public Topic() {
    super();
    partitions = Collections.emptyMap();
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public Map<String, Set<Integer>> getPartitions() {
    return partitions;
  }

  public void setPartitions(Map<String, Set<Integer>> partitions) {
    this.partitions = partitions;
  }
}
