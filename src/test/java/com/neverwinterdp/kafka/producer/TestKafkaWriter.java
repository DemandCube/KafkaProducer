package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.consumer.KafkaReader;

public class TestKafkaWriter {

  private LinkedList<String> buffer;
  private KafkaWriter writer;
  private KafkaReader reader;
  private String zkURL = "127.0.0.1:2181";
  private int id = 1;
  private int partition = 1;
  private String topic;

  @Before
  public void setUp() throws Exception {
    BasicConfigurator.configure();
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    buffer = new LinkedList<>();
    writer = new KafkaWriter(zkURL, topic, partition, id);
    reader = new KafkaReader(zkURL, topic, partition);
  }

  @Test
  public void testWriteToNonExistentTopic() throws Exception {
    topic = Long.toHexString(Double.doubleToLongBits(Math.random()));
    //  partition = new Random().nextInt(10);
    writer = new KafkaWriter(zkURL, topic, partition, id);
    try {
      writer.run();
      assertTrue("Can write to auto created topic", true);
    } catch (Exception e) {
      fail("Cannot write to new Topic");
    }
  }


  @Test
  public void testCountMessages() {
    List<String> messages = new ArrayList<>();
    int count = 20;
    for (int i = 0; i < count; i++) {
      writer.run();
    }
    while (reader.hasNext()) {
      messages.add(reader.read());
    }
    assertEquals(count, messages.size());
  }


  @Test
  public void testWriteMessageOrder() throws InterruptedException {
    //TODO write to kafka, read from kafka, read from buffer, should be equal
    int count = 20;
    for (int i = 0; i < count; i++) {
      writer.run();
      buffer.add(writer.getMessage());
    }
    assertEquals(count, buffer.size());
    System.out.println("hahaha " + buffer);
    for (int i = 0; i < count; i++) {
      assertEquals(reader.read(), buffer.pollLast());
    }
  }


  @After
  public void tearDown() throws Exception {
    writer.close();
    reader.close();
  }
}
