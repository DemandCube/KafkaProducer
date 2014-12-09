package com.neverwinterdp.scribengin.datagenerator;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.Callable;

// TODO have it retryable on success and failure
public class KafkaWriter implements Callable<Boolean> {

  private static SimpleDateFormat dateFormat = new SimpleDateFormat("mm:ss");
  private static Random random = new Random();
  private String topic;
  private int partition;
  private int writerId;
  private String zkURL;
  private long delay;

  public KafkaWriter(String zkURL, String topic, int partition, int id) {
    this.zkURL = zkURL;
    this.topic = topic;
    this.partition = partition;
    this.writerId = id;
  }

  @Override
  public Boolean call() throws Exception{
    Date now = new Date();
    System.out.println("TOPIC: " + topic + " PARTITON: " + partition + " WriterID:" + writerId
        + " TIME:" + dateFormat.format(now));

  /*  if(random.nextBoolean()== false) {
      throw new IOException();
    }
    else {
      throw new NullPointerException();
    }*/
    
    throw new NullPointerException();
  // return random.nextBoolean();
  }
}
