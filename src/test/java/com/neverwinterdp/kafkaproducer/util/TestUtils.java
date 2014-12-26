package com.neverwinterdp.kafkaproducer.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

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
    int numPartitions = 0;
    try (ZookeeperHelper helper = new ZookeeperHelper(zkURL)) {
      
      numPartitions = helper.getBrokersForTopic(topic).keySet().size();
      System.out.println("number of partitions --> "+ helper.getBrokersForTopic(topic));
    } catch (Exception e) {
      e.printStackTrace();
    }
    for (int i = 0; i < numPartitions; i++) {
      try (Consumer consumer = new Consumer(zkURL, topic, i)) {
        messages.addAll(consumer.read());
      }
    }
    return messages;
  }

  public static String createRandomTopic() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }


  public static List<Integer> convert(List<String> messages) {
    return Lists.transform(messages,
        new Function<String, Integer>() {
          @Override
          public Integer apply(String t) {
            return Integer.parseInt(t);
          }
        });
  }
}
