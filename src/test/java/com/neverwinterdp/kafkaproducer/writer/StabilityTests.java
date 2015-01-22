package com.neverwinterdp.kafkaproducer.writer;

import static com.neverwinterdp.kafkaproducer.util.Utils.printRunningThreads;
import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafkaproducer.reader.KafkaReader;
import com.neverwinterdp.kafkaproducer.servers.EmbeddedCluster;
import com.neverwinterdp.kafkaproducer.util.TestUtils;
import com.neverwinterdp.kafkaproducer.util.ZookeeperHelper;

public class StabilityTests {

  protected static final Logger logger = Logger.getLogger(StabilityTests.class);
  protected static EmbeddedCluster cluster;
  protected static ZookeeperHelper helper;
  protected static String zkURL;
  protected int duration = 4 * 5 * 1000;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    printRunningThreads();

  }

  protected void initCluster(int numOfZkInstances, int numOfKafkaInstances) throws Exception {
    cluster = new EmbeddedCluster(numOfZkInstances, numOfKafkaInstances);
    cluster.start();

    zkURL = cluster.getZkURL();
    helper = new ZookeeperHelper(zkURL);
    Thread.sleep(3000);
  }

  protected Properties initProperties() throws Exception {
    Properties props = new Properties();
    props.put("request.required.acks", "1");
    props.put("producer.type", "sync");
    return props;
  }

  @Test
  public void testOneTopicOneNode() throws Exception{
       initCluster(1, 1);
       doTest(1);
  }
  
  @Test
  public void testFiveTopicsOneNode() throws Exception{
       initCluster(1, 1);
       doTest(5);
  }
  
  @Test
  public void testOneTopicThreeNodes() throws Exception{
       initCluster(1, 3);
       doTest(1);
  }
  
  @Test
  public void testFiveTopicsThreeNodes() throws Exception{
       initCluster(1, 3);
       doTest(5);
  }
  
  public void doTest(int topicsCount) throws Exception {
    try {
      
      for (int k = 0; k < topicsCount; k++) {
        final String topic = TestUtils.createRandomTopic();
        helper.createTopic(topic, 1, 1);

        final Properties props = initProperties();
        new Thread(new Runnable() {

          @Override
          public void run() {
            KafkaWriter writer;
            try {
              writer = new KafkaWriter.Builder(zkURL, topic).properties(props).partition(0).build();

              long endTime = System.currentTimeMillis() + (duration);
              int i = 0;
              while (System.currentTimeMillis() < endTime) {
                writer.write("message" + i);
                i++;
              }
              int total = i;
              KafkaReader reader;
              reader = new KafkaReader(zkURL, topic, 0);
              List<String> messages = new LinkedList<String>();
              i = 0;
              while (reader.hasNext()) {
                messages = reader.read();
                for (String message : messages) {
                  assertEquals(message, "message" + i);
                  i++;
                }
              }
              assertEquals(total, i);

              reader.close();
            } catch (Exception e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }

          }
        });
      }
    } finally {
      cluster.shutdown();
    }
  }

}
