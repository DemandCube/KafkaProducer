package com.neverwinterdp.kafkaproducer.readerwriter;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import kafka.common.FailedToSendMessageException;
import kafka.server.KafkaServer;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.reader.KafkaReader;
import com.neverwinterdp.kafkaproducer.retry.DefaultRetryStrategy;
import com.neverwinterdp.kafkaproducer.retry.RunnableRetryer;
import com.neverwinterdp.kafkaproducer.servers.EmbeddedCluster;
import com.neverwinterdp.kafkaproducer.util.HostPort;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;
import com.neverwinterdp.kafkaproducer.writer.KafkaWriter;
import com.neverwinterdp.kafkaproducer.writer.TestKafkaWriter;

public abstract class AbstractReaderWriterTest {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  protected static final Logger logger = Logger.getLogger(TestKafkaWriter.class);
  protected static EmbeddedCluster cluster;
  protected static ZookeeperHelper helper;
  protected static String zkURL;
 
  private static FileInputStream WriterInputDocument;
  private static HSSFWorkbook writerWorkbook;
  private static FileInputStream retryerInputDocument;
  private static HSSFWorkbook retryerWorkbook;


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    printRunningThreads();
    WriterInputDocument = new FileInputStream(new File("writer_stats.xls"));
    writerWorkbook = new HSSFWorkbook(WriterInputDocument);
    retryerInputDocument = new FileInputStream(new File("retryer_stats.xls"));
    retryerWorkbook = new HSSFWorkbook(retryerInputDocument);
    

  }

  @AfterClass
  public static void setAfterClass() throws Exception {
    WriterInputDocument.close();
    FileOutputStream outputFile = new FileOutputStream(new File("writer_stats.xls"));
    writerWorkbook.write(outputFile);
    outputFile.close();
    retryerInputDocument.close();
    outputFile = new FileOutputStream(new File("retryer_stats.xls"));
    retryerWorkbook.write(outputFile);
    outputFile.close();
  }

  protected void initCluster(int numOfZkInstances, int numOfKafkaInstances) throws Exception {
    cluster = new EmbeddedCluster(numOfZkInstances, numOfKafkaInstances);
    cluster.start();
    zkURL = cluster.getZkURL();
    helper = new ZookeeperHelper(zkURL);
    Thread.sleep(3000);
  }

  private void writeAndRead() throws Exception {
    String topic = TestUtils.createRandomTopic();
    helper.createTopic(topic, 1, 1);
    Properties props = initProperties();
    KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).properties(props).build();
    writer.write("message");
    KafkaReader reader = new KafkaReader(zkURL, topic, 0);

    List<String> messages = reader.read();
    assertEquals(messages.size(), 1);
    reader.close();
  }

  protected abstract Properties initProperties() throws Exception;

  ////@Test(expected = IndexOutOfBoundsException.class)
  public void testNoServerRunning() throws Exception {
    try {
      initCluster(0, 0);
      writeAndRead();
    } finally {
      // cluster.shutdown();
    }

  }

  //@Test(expected = ZkNoNodeException.class)
  public void testOnlyZookeeperRunning() throws Exception {
    try {
      initCluster(1, 0);
      writeAndRead();
    } finally {
      cluster.shutdown();
    }
  }

  //@Test(expected = ZkTimeoutException.class)
  public void testOnlyBrokerRunning() throws Exception {
    try {
      initCluster(0, 1);
      writeAndRead();
    } finally {
      // cluster.shutdown();
    }
  }

  //@Test(expected = NullPointerException.class)
  public void testWriteToNonExistentTopic() throws Exception {
    writeToNonExistentTopic();
  }

  public void writeToNonExistentTopic() throws Exception {
    try {
      initCluster(1, 1);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(zkURL, "someTopic").properties(props).partition(99).build();
      writer.write("my message");
    } finally {
      cluster.shutdown();
    }
  }

  //@Test
  public void testWriteToWrongServer() throws Exception {

    try {
      initCluster(1, 2);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 2, 1);
      Properties props = initProperties();
      Collection<HostPort> brokers = helper.getBrokersForTopic(topic).values();
      killLeader(topic);
      brokers = helper.getBrokersForTopic(topic).values();
      KafkaWriter writer = new KafkaWriter.Builder(brokers, topic).partition(0).properties(props).build();
      writer.write("my message");
    } finally {
      cluster.shutdown();
    }
  }

  //@Test
  public void testCheckWritenDataExistOnPartition() throws Exception {
    try {
      initCluster(1, 1);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 3, 1);
      // helper.addPartitions(topic, 2);
      Properties props = initProperties();
      for (int i = 0; i < 3; i++) {
        KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).properties(props).partition(i).build();
        writer.write("message" + i);
      }

      for (int i = 0; i < 3; i++) {
        Thread.sleep(5000);
        KafkaReader reader = new KafkaReader(zkURL, topic, i);

        List<String> messages = new LinkedList<String>();
        while (reader.hasNext()) {
          messages.addAll(reader.read());
        }
        assertEquals(messages.size(), 1);
        assertEquals("message" + i, messages.get(0));
        reader.close();

      }

    } finally {
      cluster.shutdown();
    }
  }

  //@Test
  public void testWriteToSingleTopicSinglePartition() throws Exception {
    try {
      initCluster(1, 1);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 3, 1);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).properties(props).partition(0).build();
      writer.write("message");
      KafkaReader reader;
      reader = new KafkaReader(zkURL, topic, 0);
      Thread.sleep(5000);
      List<String> messages = reader.read();
      assertEquals(messages.size(), 1);
      assertEquals(messages.get(0), "message");
      for (int i = 1; i < 3; i++) {
        reader = new KafkaReader(zkURL, topic, i);
        messages = reader.read();
        assertEquals(messages.size(), 0);
      }
      reader.close();
    } finally {
      cluster.shutdown();
    }
  }

  //@Test
  public void testWriteTenThousandMessages() throws Exception {
    try {
      initCluster(1, 1);
      String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 3, 1);
      Properties props = initProperties();
      KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).properties(props).partition(0).build();
      for (int i = 0; i < 10000; i++)
        writer.write("message" + i);
      KafkaReader reader;
      reader = new KafkaReader(zkURL, topic, 0);
      List<String> messages = new LinkedList<String>();
      ;
      while (reader.hasNext()) {
        messages.addAll(reader.read());
      }
      assertEquals(messages.size(), 10000);

      for (int i = 0; i < 10000; i++) {
        assertEquals(messages.get(i), "message" + i);
      }
      reader.close();
    } finally {
      cluster.shutdown();
    }
  }

  //@Test
  public void testWriteTenThousandMessagesToFiveTopics() throws Exception {
    try {
      initCluster(1, 1);
      String[] topics = new String[5];
      Properties props = initProperties();
      for (int i = 0; i < 5; i++)
        topics[i] = TestUtils.createRandomTopic();
      for (int i = 0; i < 5; i++) {
        helper.createTopic(topics[i], 1, 1);
        KafkaWriter writer = new KafkaWriter.Builder(zkURL, topics[i]).properties(props).partition(0).build();
        for (int j = 0; j < 10000; j++)
          writer.write("message" + j);
      }
      for (int i = 0; i < 5; i++) {
        KafkaReader reader;
        reader = new KafkaReader(zkURL, topics[i], 0);
        List<String> messages = new LinkedList<String>();

        while (reader.hasNext()) {
          messages.addAll(reader.read());
          Thread.sleep(3000);
        }
        assertEquals(messages.size(), 10000);

        for (int j = 0; j < 10000; j++) {
          assertEquals(messages.get(j), "message" + j);
        }
        messages.clear();
        reader.close();
      }

    } finally {
      cluster.shutdown();
    }
  }

  //@Test
  public void testWriteTenThousandMessagesToFiveTopicsTowPartition() throws Exception {
    try {
      initCluster(1, 1);

      String[] topics = new String[5];
      for (int i = 0; i < 5; i++)
        topics[i] = TestUtils.createRandomTopic();
      int[] partitions = { 0, 1 };
      Properties props = initProperties();
      for (int i = 0; i < 5; i++) {
        helper.createTopic(topics[i], 2, 1);
        for (int partition : partitions) {
          KafkaWriter writer = new KafkaWriter.Builder(zkURL, topics[i]).properties(props).partition(partition).build();
          for (int j = 0; j < 5000; j++)
            writer.write("message" + j);
        }
      }

      for (int i = 0; i < 5; i++) {
        KafkaReader reader;
        for (int partition : partitions) {
          reader = new KafkaReader(zkURL, topics[i], partition);
          List<String> messages = new LinkedList<String>();

          while (reader.hasNext()) {
            messages.addAll(reader.read());
          }
          assertEquals(messages.size(), 5000);

          for (int j = 0; j < 5000; j++) {
            assertEquals(messages.get(j), "message" + j);
          }

          reader.close();
          messages.clear();
        }
      }

    } finally {
      cluster.shutdown();
    }
  }

  //@Test
  public void testRetryUntilKafkaStart() throws Exception {
    List<String> messages = new ArrayList<>();
    try {
      initCluster(1, 0);
      Properties props = initProperties();

      final RunnableRetryer retryer;
      final String topic = TestUtils.createRandomTopic();

      KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).partition(0).properties(props).build();

      retryer = new RunnableRetryer(new DefaultRetryStrategy(10, 3000, FailedToSendMessageException.class), writer);
      new Thread(new Runnable() {

        @Override
        public void run() {
          int retries = 0;
          do {
            retries = retryer.getRetryStrategy().getRetries();
          } while (retries < 1);
          System.out.println("Starting Kafka");
          cluster.addKafkaServer();
          helper.createTopic(topic, 1, 1);

        }
      }).start();
      retryer.run();
      Thread.sleep(5000);
      KafkaReader reader = new KafkaReader(zkURL, topic, 0);
      messages = reader.read();
      reader.close();
      System.out.println("messages size " + messages.size());
      assertEquals(1, messages.size());

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      cluster.shutdown();
    }

  }

  //@Test
  public void testRetryUntilTopicCreation() throws Exception {
    List<String> messages = new ArrayList<>();
    try {
      initCluster(1, 1);
      Properties props = initProperties();

      final RunnableRetryer retryer;
      final String topic = TestUtils.createRandomTopic();

      KafkaWriter writer = new KafkaWriter.Builder(zkURL, topic).partition(0).properties(props).build();

      retryer = new RunnableRetryer(new DefaultRetryStrategy(10, 3000, FailedToSendMessageException.class), writer);
      new Thread(new Runnable() {

        @Override
        public void run() {
          int retries = 0;
          do {
            retries = retryer.getRetryStrategy().getRetries();
            System.out.println("retries " + retries);
          } while (retries < 1);
          System.out.println("Creating Topic");
          helper.createTopic(topic, 1, 1);

        }
      }).start();
      retryer.run();
      Thread.sleep(5000);
      KafkaReader reader = new KafkaReader(zkURL, topic, 0);
      messages = reader.read();
      reader.close();
      System.out.println("messages size " + messages.size());
      assertEquals(1, messages.size());

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      cluster.shutdown();
    }

  }

  //@Test
  public void testRetryWhenLeaderKilled() throws Exception {
    try {
      initCluster(1, 3);
      List<String> messages = new ArrayList<>();
      final RunnableRetryer retryer;
      final String topic = TestUtils.createRandomTopic();
      helper.createTopic(topic, 1, 3);
      Properties props = initProperties();
      KafkaWriter.Builder builder = new KafkaWriter.Builder(zkURL, topic).partition(0).properties(props);
      KafkaWriter writer = new KafkaWriter(builder);
      retryer = new RunnableRetryer(new DefaultRetryStrategy(5, 500, FailedToSendMessageException.class), writer);

      for (int i = 0; i < 10000; i++) {
        try {
          retryer.run();
          if (i == 9)
            killLeader(topic);
        } catch (Exception e) {
          System.out.println("Exception " + e);

        }
      }
      Thread.sleep(10000);
      System.out.print("received messages");
      int received = 0;
      KafkaReader reader = new KafkaReader(zkURL, topic, 0);
      while (reader.hasNext()) {

        messages = reader.read();
        System.out.print(messages);
        received += messages.size();

      }

      System.out.println(" received is  " + received + " failed " + writer.getFailureCount());
      assertEquals(10000, received);
      reader.close();
      messages.clear();
    } finally {
      cluster.shutdown();
    }

  }

  //@Test
  public void testKillLeaderAndRebalance() throws Exception {

    int kafkaBrokers = 4;
    final int replicationFactor = 3;
    initCluster(1, kafkaBrokers);

    final String topic = TestUtils.createRandomTopic();
    try {
      helper.createTopic(topic, 1, replicationFactor);
      final HostPort leader = helper.getLeaderForTopicAndPartition(topic, 0);
      Properties props = initProperties();
      KafkaWriter.Builder builder = new KafkaWriter.Builder(zkURL, topic).partition(0).properties(props);

      KafkaWriter writer = new KafkaWriter(builder);
      RunnableRetryer retryer = new RunnableRetryer(
          new DefaultRetryStrategy(5, 500, FailedToSendMessageException.class), writer);

      for (int i = 0; i < 10000; i++) {
        retryer.run();
        if (i == 10) {
          new Thread(new Runnable() {

            @Override
            public void run() {

              try {
                System.out.println("start attack ");
                killLeader(leader);
                List<Object> remainingBrokers = new ArrayList<>();
                for (KafkaServer server : cluster.getKafkaServers()) {
                  remainingBrokers.add(server.config().brokerId());
                }
                System.out.println("Leader killed  ");
                int brokersForTopic = helper.getBrokersForTopicAndPartition(topic, 0).size();
                assertEquals(replicationFactor - 1, brokersForTopic);
                helper.rebalanceTopic(topic, 0, remainingBrokers);
                System.out.println("rebalance done ");

              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          }).start();
        }

      }

      Thread.sleep(10000);
      KafkaReader reader;
      reader = new KafkaReader(zkURL, topic, 0);
      List<String> messages = new LinkedList<String>();
      System.out.print("received messages");
      int received = 0;
      while (reader.hasNext()) {

        messages = reader.read();
        received += messages.size();

      }
      System.out.println(" received is  " + received + " failed " + writer.getFailureCount());
      assertEquals(10000, received);
      messages.clear();
      reader.close();
    } finally {
      helper.close();
      cluster.shutdown();
    }
  }

  @Test
  public void testKillLeaderRebalnceAndRestartWithRetryer() throws Exception {
    testKillLeaderRebalnceAndRestart(true);
  }

  @Test
  public void testKillLeaderRebalnceAndRestartWithoutRetryer() throws Exception {
    testKillLeaderRebalnceAndRestart(false);
  }

  public void testKillLeaderRebalnceAndRestart(boolean useRetryer) throws Exception {

    int kafkaBrokers = 4;
    final int replicationFactor = 3;
    initCluster(1, kafkaBrokers);

    final String topic = TestUtils.createRandomTopic();
    try {
      helper.createTopic(topic, 1, replicationFactor);
      final HostPort leader = helper.getLeaderForTopicAndPartition(topic, 0);
      Properties props = initProperties();
      KafkaWriter.Builder builder = new KafkaWriter.Builder(zkURL, topic).partition(0).properties(props);
      RunnableRetryer retryer = null;
      KafkaWriter writer = new KafkaWriter(builder);
      if (useRetryer)
        retryer = new RunnableRetryer(new DefaultRetryStrategy(5, 500, FailedToSendMessageException.class), writer);

      for (int i = 0; i < 10000; i++) {
        if (useRetryer)
          retryer.run();
        else
          writer.write(i + "");

        if (i == 10) {
          new Thread(new Runnable() {

            @Override
            public void run() {

              try {
                KafkaServer deadLeader = killLeader(leader);
                List<Object> remainingBrokers = new ArrayList<>();
                for (KafkaServer server : cluster.getKafkaServers()) {
                  remainingBrokers.add(server.config().brokerId());
                }
                System.out.println("Leader killed  ");
                // before rebalance the shouldn't be equal
                int brokersForTopic = helper.getBrokersForTopicAndPartition(topic, 0).size();
                assertEquals(replicationFactor - 1, brokersForTopic);
                helper.rebalanceTopic(topic, 0, remainingBrokers);
                System.out.println("rebalance done ");
                brokersForTopic = helper.getBrokersForTopicAndPartition(topic, 0).size();
                deadLeader.startup();
                cluster.getKafkaServers().add(deadLeader);
                System.out.println("dead leader started");
                remainingBrokers.clear();
                for (KafkaServer server : cluster.getKafkaServers()) {
                  remainingBrokers.add(server.config().brokerId());
                }
                helper.rebalanceTopic(topic, 0, remainingBrokers);
                System.out.println("Rebalance again done ");
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          }).start();
        }

      }
      
      Thread.sleep(10000);
      if(retryer != null){
        retryer.stop();
      }
      KafkaReader reader;
      reader = new KafkaReader(zkURL, topic, 0);
      List<String> messages = new LinkedList<String>();

      System.out.print("received messages");
      int received = 0;
      while (reader.hasNext()) {
        messages = reader.read();
        received += messages.size();
      }

      System.out.println(" received is  " + received + " failed " + writer.getFailureCount());
      if (useRetryer){
        assertEquals(10000, received);
        updateSpreadSheet(retryerWorkbook, 0, received , writer.getFailureCount());
      }else {
        assertTrue(received < 10000);
        updateSpreadSheet(writerWorkbook, 0,  received , writer.getFailureCount());
      }
      
      messages.clear();
      reader.close();
    } finally {
      helper.close();
      cluster.shutdown();
    }
  }
protected void updateSpreadSheet(HSSFWorkbook workbook, int sheet, int received, int failures ){
  HSSFSheet my_worksheet = workbook.getSheetAt(sheet);
  Cell cell = null;
  int row = getRow();
  cell = my_worksheet.getRow(row).getCell(1);
  cell.setCellValue(received);
  cell = my_worksheet.getRow(row).getCell(2);
  cell.setCellValue(failures);
  cell = my_worksheet.getRow(row).getCell(3);
  cell.setCellValue(10000 - (received + failures));
}
  protected abstract int getRow();
  
  protected void killLeader(String topic) throws Exception {
    // while writer threads are writing, kill the leader
    HostPort leader = helper.getLeaderForTopicAndPartition(topic, 0);
    for (KafkaServer server : cluster.getKafkaServers()) {
      if (leader.getHost().equals(server.config().hostName()) && leader.getPort() == server.config().port()) {
        server.shutdown();
        server.awaitShutdown();
        System.out.println("Shutting down current leader --> " + server.config().hostName() + ":"
            + server.config().port());
      }
    }
  }

  protected KafkaServer killLeader(HostPort leader) throws Exception {

    KafkaServer kafkaServer = null;
    for (KafkaServer server : cluster.getKafkaServers()) {
      if (leader.getHost().equals(server.config().hostName()) && leader.getPort() == server.config().port()) {
        server.shutdown();
        server.awaitShutdown();

        kafkaServer = server;
        System.out.println("Shutting down current leader --> " + server.config().hostName() + ":"
            + server.config().port() + " id " + server.config().brokerId());
      }
    }

    cluster.getKafkaServers().remove(kafkaServer);
    return kafkaServer;
  }

}
