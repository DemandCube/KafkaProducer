package com.neverwinterdp.kafkaproducer.messagegenerator;

import java.util.concurrent.atomic.AtomicInteger;

import com.neverwinterdp.kafkaproducer.partitioner.OddEvenPartitioner;

import kafka.producer.Partitioner;

// Used for tests in conjunction with the OddEvenPartitioner
public class IntegerGenerator implements MessageGenerator<String> {

  private AtomicInteger currNum;
  private Class<? extends Partitioner> partitionerClass = OddEvenPartitioner.class;

  public IntegerGenerator() {
    currNum = new AtomicInteger(0);
  }

  @Override
  public String next() {
    return Integer.toString(currNum.getAndIncrement());
  }

  @Override
  public boolean hasNext() {
    return currNum.get() < Integer.MAX_VALUE;
  }

  @Override
  public void remove() {
    currNum.decrementAndGet();
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
