package com.neverwinterdp.kafka.producer;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.producer.servers.Server;
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
    KafkaWriteWorker[] worker = new  KafkaWriteWorker[3];
    ExecutorService executorPool = Executors.newFixedThreadPool(worker.length);
    for(int i = 0; i < worker.length; i++) {
      worker[i] = new  KafkaWriteWorker("kafka-writer-" + (i + 1), 5) ;
      executorPool.execute(worker[i]);
    }
    executorPool.shutdown();
    
    Server[] servers = cluster.getKafkaServers();
    Random rand = new Random();
    for(int i = 0; i < 5; i++) {
      int idx = rand.nextInt(servers.length);
      servers[idx].shutdown();
      Thread.sleep(1000);
      servers[idx].start();
      Thread.sleep(1000);
    }
    
    executorPool.awaitTermination(3 * 1000, TimeUnit.MILLISECONDS);
    System.out.println("\n\n");
    banner("Worker Report:");
    for(int i = 0; i < worker.length; i++) {
      System.out.println("Worker " + worker[i].name + " sent " + worker[i].sentCount + " messages");
    }
    System.out.println("\n\n");
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
  
  public class KafkaWriteWorker implements Runnable {
    private String name ;
    private long   period ;
    private int    sentCount;
    
    public KafkaWriteWorker(String name, long period) {
      this.name = name;
      this.period = period;
    }
    
    @Override
    public void run() {
      try {
        TuanKafkaWriter writer = new TuanKafkaWriter(name, cluster.getKafkaConnect()) ;
        while(true) {
          sentCount++;
          String hello = "Hello " + sentCount;
          writer.send("hello", hello) ;
          Thread.sleep(period);
        }
      } catch(InterruptedException ex) {
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
    
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
