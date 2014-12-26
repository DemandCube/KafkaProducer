package com.neverwinterdp.kafkaproducer.messagegenerator;

import com.neverwinterdp.kafkaproducer.partitioner.OddEvenPartitioner;

import kafka.producer.Partitioner;

// Used for tests in conjunction with the OddEvenPartitioner
public class IntegerGenerator implements MessageGenerator<String> {

  private int currNum;
  private Class<? extends Partitioner> partitionerClass = OddEvenPartitioner.class;

  public IntegerGenerator() {
    currNum = 0;
  }

  @Override
  public String next() {
    return Integer.toString(currNum++);
  }

  @Override
  public boolean hasNext() {
    return currNum < Integer.MAX_VALUE;
  }

  @Override
  public void remove() {
    currNum--;
  }

  @Override
  public Class<? extends Partitioner> getPartitionerClass() {
    return partitionerClass;
  }

  @Override
  public void setPartitionerClass(Class<? extends Partitioner> partitionerClass) {
    this.partitionerClass = partitionerClass;
  }
}
