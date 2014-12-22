package com.neverwinterdp.kafka.servers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Test;

public class TestMyCluster {

  static {
    BasicConfigurator.configure();
    // System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  @Test
  public void testClusterStarts() {
    MyCluster cluster = new MyCluster(1, 1);
    try {
      cluster.start();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unable to start Kafka Cluster");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testZkStarts() {
    MyCluster cluster = new MyCluster(1, 1);
    try {
      cluster.start();
      assertEquals(1, cluster.getZkHosts().size());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unable to start Kafka Cluster");
    } finally {
      cluster.shutdown();
    }    
  }

  @Test
  public void testClusterShutdown() {
    MyCluster cluster = new MyCluster(1, 1);
    try {
      cluster.start();
      cluster.shutdown();
    } catch (Exception e) {
      Assert.fail("Unable to start Kafka Cluster");
    }
  }

  @Test
  public void testCountKafkaBrokers() throws Exception {
    int kafkaBrokers = 3;
    MyCluster cluster = new MyCluster(1, kafkaBrokers);
    cluster.start();
    assertEquals(kafkaBrokers, cluster.getKafkaServers().size());
  }

  /**
   * Checks whether a specified port is occupied.
   * 
   * @param host
   *        - the interface/host to test.
   * @param port
   *        - port to check.
   * @return - true if port is occupied.
   */
  public static boolean isPortOccupied(final int port) {
    final Socket sock = new Socket();
    try {
      sock.connect(new InetSocketAddress("127.0.01", port));
      sock.close();
      return true;
    } catch (final IOException e1) {
      return false;
    } finally {
      try {
        sock.close();
      } catch (final IOException e) {
        // ignore
      }
    }
  }
}