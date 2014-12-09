package com.neverwinterdp.scribengin.datagenerator;

import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import org.apache.curator.test.TestingServer;


public class Servers {
  TestingServer zookeeper;
  KafkaServer server;

  public void start() throws Exception {
    zookeeper = new TestingServer(2180);
    zookeeper.start();

    Properties props = null;
    KafkaConfig kafkaConfig = new KafkaConfig(props);

    server = new kafka.server.KafkaServer(kafkaConfig, new Time() {

      @Override
      public void sleep(long arg0) {
         }

      @Override
      public long nanoseconds() {
            return 0;
      }

      @Override
      public long milliseconds() {
          return 0;
      }
    });
    server.startup();
  }

  public void stop() throws Exception {
    zookeeper.stop();
    server.shutdown();
  }
}
