package com.neverwinterdp.kafka.producer;

import com.neverwinterdp.kafka.producer.messagegenerator.MessageGenerator;
import com.neverwinterdp.kafka.producer.writer.KafkaWriter;

public class KafkaWriterRunnable implements Runnable{
  private KafkaWriter writer;
  private MessageGenerator messageGen;
  
  public KafkaWriterRunnable(KafkaWriter writer, MessageGenerator messageGenerator) {
    this.messageGen = messageGenerator;
    //this.partitionerClass = messageGenerator.getPartitionerClass();
    this.writer = writer;
  }
  
  @Override
  public void run() {
    try {
      this.writer.send(messageGen.next());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
