package com.neverwinterdp.kafkaproducer.reader;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import kafka.server.KafkaServer;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.reader.KafkaReader;
import com.neverwinterdp.kafkaproducer.servers.EmbeddedCluster;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;
import com.neverwinterdp.kafkaproducer.writer.KafkaWriter;

public abstract class AbstractReaderTest {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  protected static final Logger logger = Logger.getLogger(AbstractReaderTest.class);
  protected static EmbeddedCluster cluster;
  protected static ZookeeperHelper helper;
  protected static String zkURL;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    printRunningThreads();

  }

  protected void initCluster(int numOfZkInstances, int numOfKafkaInstances) throws Exception {
    cluster = new EmbeddedCluster(numOfZkInstances, numOfKafkaInstances);
    cluster.start();
    zkURL = cluster.getZkURL();
    helper = new ZookeeperHelper(zkURL);
    Thread.sleep(3000);
  }
  
  private void writeAndRead() throws Exception{
    String topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);
    KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).build();
    writer.write("message");
    KafkaReader reader = new KafkaReader(zkURL, topic, 0);
    Properties props = initProperties();
    props.put("zookeeper.connect", zkURL);
    List<String> messages = reader.read();
    assertEquals(messages.size(), 1);
    reader.close();
    
  }

  protected abstract Properties initProperties() throws Exception;

  @Test(expected = IndexOutOfBoundsException.class)
  public void testNoServerRunning() throws Exception {
    try {
      initCluster(0, 0);
      writeAndRead();

    } finally {
      // cluster.shutdown();
    }

  }

  @Test(expected = ZkNoNodeException.class)
  public void testOnlyZookeeperRunning() throws Exception {
    try {
      initCluster(1, 0);
      writeAndRead();

    } finally {
      cluster.shutdown();
    }
  }

  @Test(expected = ZkTimeoutException.class)
  public void testOnlyBrokerRunning() throws Exception {
    try {
      initCluster(0, 1);
      writeAndRead();

    } finally {
      // cluster.shutdown();
    }
  }

  @Test
  public void testWriteToNonExistentTopic() throws Exception {
      writeToNonExistentTopic();
  }

  public void writeToNonExistentTopic() throws Exception {
    try {
      initCluster(1, 1);
      writeAndRead();

    } finally {
      cluster.shutdown();
    }
  }

  
}
