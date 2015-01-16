package com.neverwinterdp.kafkaproducer.util;

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
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

// TODO get the names right
// TODO tests
// TODO sorround in retry block
public class ZookeeperHelper implements Closeable {

  private static final Logger logger = Logger.getLogger(ZookeeperHelper.class);
  private final static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

  private String brokerInfoLocation = "/brokers/ids/";
  private String topicInfoLocation = "/brokers/topics/";

  private String zkConnectString;
  private CuratorFramework zkClient;
  private PathChildrenCache pathChildrenCache;

  public ZookeeperHelper(String zookeeperURL) {
    super();
    zkConnectString = zookeeperURL;
    zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        retryPolicy);
    init();
  }

  private void init() {
    zkClient.start();
    try {
      zkClient.blockUntilConnected();
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }
  }

  public HostPort getLeaderForTopicAndPartition(String topic, int partition) throws Exception {

    String[] values = getLeader(topic, partition).split(":");
    if (values.length == 2)
      return new HostPort(values[0], values[1]);
    else
      return null;
  }

  private String getLeader(String topic, int partition) throws Exception {
    String leader = "";

    PartitionState partitionState = getPartitionState(topic, partition);
    int leaderId = partitionState.getLeader();
    byte[] bytes;

    try {
      if (leaderId == -1) {
        return leader;
      }
      logger.debug("Going to look for " + brokerInfoLocation + leaderId);
      bytes = zkClient.getData().forPath(brokerInfoLocation + leaderId);
    } catch (NoNodeException nne) {
      logger.error(nne.getMessage(), nne);
      return leader;
    }
    Broker part = Utils.toClass(bytes, Broker.class);
    logger.debug("leader " + part);

    return leader.concat(part.getHost()).concat(":").concat(String.valueOf(part.getPort()));
  }

  /*
   * /brokers/[0...N] --> { "host" : "host:port",
   * "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
   * "topicN": ["partition1" ... "partitionN"] } }
   */
  public Collection<HostPort> getBrokersForTopicAndPartition(String topic, int partition)
      throws Exception {
    PartitionState partitionState = getPartitionState(topic, partition);
    Collection<HostPort> brokers = new LinkedList<HostPort>();
    HostPort broker;
    byte[] partitions;
    logger.debug("PartitionState " + partitionState);

    for (Integer b : partitionState.getIsr()) {
      // if broker is registered but offline next line throws
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
    // TODO exit if data is not a json obj
    logger.info("writeData. path: " + path + " data: " + Arrays.toString(data));
    if (zkClient.checkExists().forPath(path) == null) {

      String created =
          zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
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
        // if broker is registered but offline next line throws nonodeexception
        try {
          byte[] partitions = zkClient.getData().forPath(brokerInfoLocation + replicaID);
          Broker part = Utils.toClass(partitions, Broker.class);
          hostPort = new HostPort(part.getHost(), part.getPort());
          brokers.put(Integer.parseInt(topicPartition.getKey()), hostPort);
        } catch (NoNodeException nne) {
          logger.debug(nne.getMessage());
        }
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
  private PartitionState getPartitionState(String topic, int partion) throws Exception {
    try {
      zkClient.getData().forPath(topicInfoLocation + topic);
    } catch (NoNodeException nne) {
      // there are no nodes for the topic. We return an empty
      // Partitionstate
      logger.error(nne.getMessage());
      return new PartitionState();
    }
    byte[] bytes =
        zkClient.getData().forPath(topicInfoLocation + topic + "/partitions/" + partion + "/state");
    PartitionState partitionState = Utils.toClass(bytes, PartitionState.class);
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

  // write to
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


  // Listener for node changes
  public void setTopicNodeListener(TopicNodeListener topicNodeListener) throws Exception {
    logger.info("setTopicNodeListener. ");
    pathChildrenCache =
        new PathChildrenCache(zkClient, topicInfoLocation + topicNodeListener.getTopic(),
            true);
    pathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE);
    pathChildrenCache.getListenable().addListener(topicNodeListener);
  }


  @Override
  public void close() throws IOException {
    if (pathChildrenCache != null)
      pathChildrenCache.close();
    zkClient.close();
  }

  public void addPartitions(String topic, int partitions) {
    ZkClient client = new ZkClient(zkConnectString, 10000, 10000, ZKStringSerializer$.MODULE$);
    AdminUtils.addPartitions(client, topic, partitions, "", false, new Properties());
    client.close();
  }

  public void createTopic(String topic, int partitions, int replicationFactor) {
    ZkClient client = new ZkClient(zkConnectString, 10000, 10000, ZKStringSerializer$.MODULE$);
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(client, topic, partitions, replicationFactor, topicConfig);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // Hack
    }
    client.close();
  }

  public void deleteTopic(String topic) {
    ZkClient client = new ZkClient(zkConnectString, 10000);
    client.deleteRecursive(ZkUtils.getTopicPath(topic));
    client.close();
  }
}
