package com.neverwinterdp.kafka.producer.messagegenerator;


public interface MessageGenerator {
  //void setPartitioner(Partitioner p);
  String next();
}
