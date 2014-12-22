package com.neverwinterdp.kafkaproducer.writer;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.messagegenerator.SampleMessageGenerator;
import com.neverwinterdp.kafkaproducer.servers.KafkaCluster;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class MyTest {
  
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }
  private String zkURL = "127.0.0.1:2181";
  private KafkaWriter writer;
  private KafkaCluster cluster;
  private int partition = 1;
  private String topic;

  @Before
  public void setUp() throws Exception {
    BasicConfigurator.configure();
    printRunningThreads();
    cluster = new KafkaCluster("./build/KafkaCluster", 1, 3);
    cluster.start();
  }

  /**
   * Write 100 messages to a non existent topic. If no Exception thrown then we are good.
   * */
  @Test
  public void testWriteToNonExistentTopic() throws Exception {
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    partition = new Random().nextInt(1);
    
    writer = new KafkaWriter(zkURL, topic, partition, 1);
    writer.setMessageGenerator(new SampleMessageGenerator(topic, partition, 0));
    try {
      for (int i = 0; i < 100; i++) {
        writer.run();
      }
      assertTrue("Can write to auto created topic", true);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Cannot write to new Topic "+ e);
    }
  }
  

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    printRunningThreads();
  }

}