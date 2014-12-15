package com.neverwinterdp.kafka.producer.partitioners;

// ukipika unafanya hilton ikae kama kibanda
import kafka.producer.Partitioner;

/**
 * Always writes to partition 1
 * */
public class SamePartitionPartitioner implements Partitioner {

  @Override
  public int partition(Object arg0, int arg1) {
    return 1;
  }
}
