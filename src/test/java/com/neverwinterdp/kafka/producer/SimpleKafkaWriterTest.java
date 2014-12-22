package com.neverwinterdp.kafka.producer;


import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafka.producer.consumer.KafkaReader;
import com.neverwinterdp.kafka.producer.writer.KafkaWriter;
import com.neverwinterdp.kafka.servers.MyCluster;

public class SimpleKafkaWriterTest {
  private static MyCluster cluster;
  private static String topic = "SimpleKafkaWriterTest";;
  
  @BeforeClass
  public static void setup() throws Exception{
    cluster = new MyCluster(1, 1);
    cluster.start();
  }
  
  @AfterClass
  public static void teardown(){
    cluster.shutdown();
  }
  
  @Test
  public void testWrite() throws Exception{
    //Write data
    KafkaWriter writer = new KafkaWriter("test", cluster.getKafkaBrokersListAsString(), topic);
    assertEquals("test", writer.getName());
    for(int i = 0; i < 10; i++){
      writer.send(Integer.toString(i));
    }
    
    //Make sure data written is data read in
    KafkaReader consumer = new KafkaReader(cluster.getZookeeperListAsString(), topic,0);
    consumer.initialize();
    List<String> messages = consumer.read();
    consumer.close();
    
    assertEquals(10, messages.size());
    for(int i = 0; i < messages.size(); i++){
      assertEquals(Integer.toString(i), messages.get(i));
    }
  }
}
