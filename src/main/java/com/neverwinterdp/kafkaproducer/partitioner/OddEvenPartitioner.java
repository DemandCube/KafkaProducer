package com.neverwinterdp.kafkaproducer.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Partitioner used for tests only.
 */
public class OddEvenPartitioner implements Partitioner {

  public OddEvenPartitioner(VerifiableProperties props) {}

  /**
   * Odd numbers to one partition, even numbers to the other
   */
  @Override
  public int partition(Object key, int numPartitions) {
    int number = Integer.parseInt(key.toString());
  
    // return number %2;
    if (number % 2 == 0)
      return 0;
    else {
      return 1;
    }
  }
}
