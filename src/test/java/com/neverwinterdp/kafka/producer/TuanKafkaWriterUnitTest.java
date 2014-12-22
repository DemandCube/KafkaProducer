package com.neverwinterdp.kafka.producer;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.servers.KafkaCluster;
import com.neverwinterdp.kafkaproducer.servers.Server;
import com.neverwinterdp.kafkaproducer.writer.TuanKafkaWriter;

public class TuanKafkaWriterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    printRunningThreads();
    cluster = new KafkaCluster("./build/KafkaCluster", 1, 3);
    cluster.start();
  }

  @Test
  public void testKafkaBasicOperations() throws Exception {
    banner("Test Kafka Basic Operations");
    KafkaWriteWorker[] worker = new KafkaWriteWorker[3];
    ExecutorService executorPool = Executors.newFixedThreadPool(worker.length);
    for (int i = 0; i < worker.length; i++) {
      worker[i] = new KafkaWriteWorker("kafka-writer-" + (i + 1), 5);
      executorPool.execute(worker[i]);
    }
    executorPool.shutdown();

    Server[] servers = cluster.getKafkaServers();
    Random rand = new Random();
    for (int i = 0; i < 5; i++) {
      int idx = rand.nextInt(servers.length);
      servers[idx].shutdown();
      Thread.sleep(1000);
      servers[idx].start();
      Thread.sleep(1000);
    }

    executorPool.awaitTermination(3 * 1000, TimeUnit.MILLISECONDS);
    System.out.println("\n\n");
    banner("Worker Report:");
    for (int i = 0; i < worker.length; i++) {
      System.out.println("Worker " + worker[i].name + " sent " + worker[i].sentCount + " messages");
    }
    System.out.println("\n\n");
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    printRunningThreads();
  }

  private void banner(String title) {
    System.out.println(title);
    System.out.println("--------------------------------------------------------------");
  }

  public class KafkaWriteWorker implements Runnable {
    private String name;
    private long period;
    private int sentCount;

    public KafkaWriteWorker(String name, long period) {
      this.name = name;
      this.period = period;
    }

    @Override
    public void run() {
      try {
        TuanKafkaWriter writer = new TuanKafkaWriter(name, cluster.getKafkaConnect());
        while (true) {
          sentCount++;
          String hello = "Hello " + sentCount;
          writer.send("hello", hello);
          Thread.sleep(period);
        }
      } catch (InterruptedException ex) {
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
