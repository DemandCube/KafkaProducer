package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.consumer.KafkaReader;

public class TestKafkaWriter {

  private LinkedBlockingQueue<String> buffer;
  private KafkaWriter writer;
  private KafkaReader reader;
  private String zkURL;
  private int id = 1;
  private int partition = 1;
  private String topic = "test";

  @Before
  public void setUp() throws Exception {
    buffer = new LinkedBlockingQueue<>();
    writer = new KafkaWriter(zkURL, topic, partition, id);
    reader = new KafkaReader(zkURL, topic, partition, id);
  }

  @Test
  public void testWriteToNonExistentTopic() throws Exception {
    topic = UUID.randomUUID().toString();
    partition = new Random().nextInt(10);
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
      buffer.put(topic);
    }
    assertEquals(count, buffer.size());
    for (int i = 0; i < count; i++) {
      assertEquals(reader.read(), buffer.remove());
    }
  }


  @After
  public void tearDown() throws Exception {
    writer.close();
    reader.close();
  }
}
