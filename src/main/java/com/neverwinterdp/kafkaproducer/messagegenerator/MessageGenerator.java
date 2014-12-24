package com.neverwinterdp.kafkaproducer.messagegenerator;

import java.util.Iterator;

import kafka.producer.Partitioner;

/**
 * A source of messages for a KafkaWriter.
 * It optionally contains a partitioner that instructs kafka how to distribute
 * the messages to the various partitions
 */
public interface MessageGenerator<T> extends Iterator<T> {

  Class<? extends Partitioner> getPartitionerClass();

  void setPartitionerClass(Class<? extends Partitioner> partitionerClass);

}
