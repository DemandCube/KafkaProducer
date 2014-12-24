package com.neverwinterdp.kafkaproducer.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import com.neverwinterdp.kafkaproducer.reader.MyConsumer;

public class TestUtils {

  public static boolean isPortOpen(int port) {
    Socket socket = new Socket();
    try {
      socket.connect(new InetSocketAddress("127.0.0.1", port));
      socket.close();
      return true;
    } catch (IOException IOE) {
      return false;
    } finally {
      try {
        socket.close();
      } catch (IOException IOE) {
        // ignore
      }
    }
  }

  public static List<String> readMessages(String topic, String zkURL) {
    List<String> messages = new LinkedList<>();
    int partitions = 0;
    try (ZookeeperHelper helper = new ZookeeperHelper(zkURL)) {
      partitions = helper.getBrokersForTopic(topic).size();
    } catch (Exception e) {
      e.printStackTrace();
    }
    for (int i = 0; i < partitions; i++) {
      try (MyConsumer consumer = new MyConsumer(zkURL, topic, i)) {
        messages.addAll(consumer.read());
      }
    }
    return messages;
  }

  public static String createRandomTopic() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
