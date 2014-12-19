package com.neverwinterdp.kafka.producer.messagegenerator;

import java.text.SimpleDateFormat;
import java.util.Date;

public class KafkaInfoTimeStampGenerator implements MessageGenerator{
  private String topic;
  //private String parition;
  private String writerId;
  
  public KafkaInfoTimeStampGenerator(String topic, String writerId){
    this.topic = topic;
    //this.parition = partition;
    this.writerId = writerId;
  }
  
  

  @Override
  public String next() {
    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSSS");
    String message =
        "TOPIC:" + this.topic + ", WriterID:" + this.writerId + 
        ", TIME:" + dateFormat.format(new Date());
    return message;
  }

}
