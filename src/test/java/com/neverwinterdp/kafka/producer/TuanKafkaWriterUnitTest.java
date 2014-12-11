package com.neverwinterdp.kafka.producer;

import java.io.File;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.producer.servers.TuanKafkaCluster;

public class TuanKafkaWriterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties") ;
  }
  
  private TuanKafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    deleteDirectory(new File("./build/KafkaCluster"));
    printRunningThreads();
    cluster = new TuanKafkaCluster("./build/KafkaCluster", 1, 3);
    cluster.start();
  }

  @Test
  public void testKafkaBasicOperations() throws Exception {
    banner("Test Kafka Basic Operations");
    TuanKafkaWriter writer = new TuanKafkaWriter("kafka-writer-1", cluster.getKafkaConnect()) ;
    for(int i = 0 ; i < 100; i++) {
      String hello = "Hello " + (i + 1) ;
      System.out.println("send: " + hello);
      writer.send("hello", hello) ;
    }
    Thread.sleep(1000);
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    printRunningThreads();
  }
  
  private void printRunningThreads() {
    banner("Running Threads");
    int count = 0;
    Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
    for(Map.Entry<Thread, StackTraceElement[]> entry : threads.entrySet()) {
      System.out.println(++count + ".Thread: " + entry.getKey().getName()) ;
    }
  }
  
  private void banner(String title) {
    System.out.println(title);
    System.out.println("--------------------------------------------------------------");
  }
  
  public static boolean deleteDirectory(File directory) {
    if(directory.exists()){
      File[] files = directory.listFiles();
      if(null!=files){
        for(int i=0; i<files.length; i++) {
          if(files[i].isDirectory()) {
            deleteDirectory(files[i]);
          }
          else {
            files[i].delete();
          }
        }
      }
    }
    return(directory.delete());
  }

}
