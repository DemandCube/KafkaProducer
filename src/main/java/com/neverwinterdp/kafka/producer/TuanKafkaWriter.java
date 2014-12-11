package com.neverwinterdp.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class TuanKafkaWriter  {
  private String name ;
  private Properties kafkaProperties ;
  private Producer<String, String> producer;
  private AtomicLong idTracker = new AtomicLong();

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public TuanKafkaWriter(String name, String kafkaBrokerUrls) {
    this(name, null, kafkaBrokerUrls) ;
  }
  
  public TuanKafkaWriter(String name, Map<String, String> props, String kafkaBrokerUrls) {
    this.name = name;
    Properties kafkaProps = new Properties() ;
    kafkaProps.put("serializer.class",  "kafka.serializer.StringEncoder");
    kafkaProps.put("partitioner.class", TuanSimplePartitioner.class.getName());
    kafkaProps.put("request.required.acks", "1");
    kafkaProps.put("metadata.broker.list", kafkaBrokerUrls);
    if(props != null) {
      kafkaProps.putAll(props);
    }
    System.out.println("Kafka Producer: " + kafkaProps);
    this.kafkaProperties = kafkaProps ;
    reconnect() ;
  }

  public void reconnect() {
    if(producer != null) producer.close(); 
    ProducerConfig config = new ProducerConfig(kafkaProperties);
    producer = new Producer<String, String>(config);
  }
  
  public void send(String topic, String data) throws Exception {
    String key = name + idTracker.getAndIncrement();
    producer.send(new KeyedMessage<String, String>(topic, key, data));
  }
  
  public void send(String topic, List<String> dataHolder) throws Exception {
    List<KeyedMessage<String, String>> holder = new ArrayList<KeyedMessage<String, String>>() ;
    for(int i = 0; i < dataHolder.size(); i++) {
      String key = name + idTracker.getAndIncrement();
      holder.add(new KeyedMessage<String, String>(topic, key, dataHolder.get(i))) ;
    }
    producer.send(holder);
  }
  
  public void close() { producer.close() ; }
}
