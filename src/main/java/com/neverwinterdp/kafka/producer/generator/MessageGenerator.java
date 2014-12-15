package com.neverwinterdp.kafka.producer.generator;

import java.util.Iterator;

import kafka.producer.Partitioner;

public interface MessageGenerator<T> extends Iterator<T>{

  Class<? extends Partitioner> getPartitionerClass();

  void setPartitionerClass(Class<? extends Partitioner> partitionerClass);

}
