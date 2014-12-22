package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafka.producer.consumer.KafkaReader;
import com.neverwinterdp.kafka.producer.messagegenerator.IntegerGenerator;
import com.neverwinterdp.kafka.producer.messagegenerator.MessageGenerator;
import com.neverwinterdp.kafka.producer.writer.KafkaWriter;
import com.neverwinterdp.kafka.servers.MyCluster;

public class SimpleRunnableKafkaWriterTest {
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
  public void testRunnableWrite() throws Exception{
  //Write data
    KafkaWriter writer = new KafkaWriter("test", cluster.getKafkaBrokersListAsString(), topic);
    MessageGenerator intGen = new IntegerGenerator();
    assertEquals("test", writer.getName());
    
    KafkaWriterRunnable runnableProducer = new KafkaWriterRunnable(writer, intGen);
    
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    final ScheduledFuture<?> timeHandle =
        scheduler.scheduleAtFixedRate(runnableProducer, 0, 100, TimeUnit.MILLISECONDS);

    scheduler.schedule(new Runnable() {
      public void run() {
        timeHandle.cancel(false);
      }
    }, 1, TimeUnit.SECONDS);
    
    scheduler.awaitTermination(1, TimeUnit.SECONDS);
    
    //Make sure data written is data read in
    KafkaReader consumer = new KafkaReader(cluster.getZookeeperListAsString(), topic,0);
    consumer.initialize();
    List<String> messages = consumer.read();
    consumer.close();
    
    
    assertTrue(messages.size() > 1);
    for(int i = 0; i < messages.size(); i++){
      assertEquals(Integer.toString(i), messages.get(i));
    }
  }
}
