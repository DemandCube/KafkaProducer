package com.neverwinterdp.kafka.producer.servers;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import com.neverwinterdp.kafka.producer.util.Utils;

/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class KafkaServerLauncher implements Server {
  private KafkaServer server;
  ThreadGroup kafkaGroup;
  int kafkaGroupTracker = 1;
  private Properties properties = new Properties();

  public KafkaServerLauncher(int id, String dataDir, int port, int replication) {
    Map<String, String> props = new HashMap<String, String>();
    props.put("broker.id", Integer.toString(id));
    props.put("port", Integer.toString(port));
    props.put("log.dirs", dataDir);
    properties.put("default.replication.factor", Integer.toString(replication));
    init(props);
  }
  
  public KafkaServerLauncher(Map<String, String> overrideProperties) {
    init(overrideProperties);
  }

  void init(Map<String, String> overrideProperties) {
    properties.put("port", "9092");
    properties.put("broker.id", "1");
    properties.put("auto.create.topics.enable", "true");
    properties.put("log.dirs", "./build/data/kafka");
    //props.setProperty("enable.zookeeper", "true");
    properties.put("zookeeper.connect", "127.0.0.1:2181");
    properties.put("default.replication.factor", "1");
    properties.put("controlled.shutdown.enable", "true");
    properties.put("auto.leader.rebalance.enable", "true");
    properties.put("controller.socket.timeout.ms", "90000");
    properties.put("controlled.shutdown.enable", "true");
    properties.put("controlled.shutdown.max.retries", "3");
    properties.put("controlled.shutdown.retry.backoff.ms", "60000");
        
    if (overrideProperties != null) {
      properties.putAll(overrideProperties);
    }
  }

  public void start() throws Exception {
    kafkaGroup =
        new ThreadGroup("Kafka-" + properties.getProperty("broker.id") + "-" + ++kafkaGroupTracker);
    String logDir = properties.getProperty("log.dirs");
    logDir = logDir.replace("/", File.separator);
    properties.setProperty("log.dirs", logDir);

    System.out.println("kafka properties:\n" + Utils.toJson(properties));

    Thread thread = new Thread(kafkaGroup, "KafkaLauncher") {
      public void run() {
        server = new KafkaServer(new KafkaConfig(properties), new SystemTime());
        server.startup();
      }
    };
    thread.start();
    //Wait to make sure the server is launched
    Thread.sleep(1000);
  }

  @Override
  public void shutdown() {
    if (server == null)
      return;
    long startTime = System.currentTimeMillis();
    //server.awaitShutdown();
    //server.socketServer().shutdown();
    //server.kafkaController().shutdown();
    //server.kafkaScheduler().shutdown();
    //server.replicaManager().shutdown() ;
    //kafkaGroup.interrupt() ;
    server.shutdown();
    //server.kafkaController().shutdown();
    //server.replicaManager().replicaFetcherManager().closeAllFetchers();
    //server.kafkaScheduler().shutdown();
    //server.logManager().shutdown();
    kafkaGroup.interrupt();
    kafkaGroup = null;
    server = null;
    System.out.println("KafkaThreadKiller thread shutdown kafka successfully");
    System.out
        .println("Shutdown KafkaServer in " + (System.currentTimeMillis() - startTime) + "ms");
  }

  static public class SystemTime implements Time {
    public long milliseconds() {
      return System.currentTimeMillis();
    }

    public long nanoseconds() {
      return System.nanoTime();
    }

    public void sleep(long ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
      }
    }
  }

  @Override
  public String getHost() {
    return server.config().advertisedHostName();
  }

  @Override
  public int getPort() {
    return server.config().advertisedPort();
  }
}
