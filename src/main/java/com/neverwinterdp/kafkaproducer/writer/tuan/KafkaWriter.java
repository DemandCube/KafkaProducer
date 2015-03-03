package com.neverwinterdp.kafkaproducer.writer.tuan;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class KafkaWriter implements Runnable {
  private String name;
  private Properties kafkaProperties;
  private KafkaProducer<String, String> producer;
  private AtomicLong idTracker = new AtomicLong();
  private static AtomicInteger counter = new AtomicInteger();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public KafkaWriter(String name, String kafkaBrokerUrls) {
    this(name, null, kafkaBrokerUrls);
  }

  public KafkaWriter(String name, Map<String, String> props, String kafkaBrokerUrls) {
    this.name = name;
    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", kafkaBrokerUrls);
    kafkaProps.put("value.serializer", StringSerializer.class.getName());
    kafkaProps.put("key.serializer", StringSerializer.class.getName());
    if (props != null) {
      kafkaProps.putAll(props);
    }
    this.kafkaProperties = kafkaProps;
    reconnect();
  }

  public void reconnect() {
    if (producer != null)
      producer.close();
    producer = new KafkaProducer<String, String>(kafkaProperties);
  }

  public void send(String topic, String data) throws Exception {
    String key = name + idTracker.getAndIncrement();
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, data);
    producer.send(record);
  }

  public void send(String topic, String key, String data) throws Exception {
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, data);
    producer.send(record);
  }

  public void send(String topic, int partition, String key, String data) throws Exception {
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition,
        key, data);
    producer.send(record);
  }

  public <T> void send(String topic, T obj) throws Exception {
    String json = com.neverwinterdp.kafkaproducer.util.Utils.toJson(obj);
    send(topic, json);
  }

  public void send(String topic, List<String> dataHolder) throws Exception {
    for (int i = 0; i < dataHolder.size(); i++) {
      String key = name + idTracker.getAndIncrement();
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,
          dataHolder.get(i));
      producer.send(record);
    }
  }

  public void close() {
    producer.close();
  }

  // The following two methods are here just for testing purposes
  @Override
  public void run() {
    String data = "test-message-" + counter.incrementAndGet();
    try {
      System.out.println("attempting to write "+ data);
      send(name, data);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public AtomicInteger getCounter() {
    return counter;
  }
}
