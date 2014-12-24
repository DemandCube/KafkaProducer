package com.neverwinterdp.kafkaproducer.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class TuanSimplePartitioner implements Partitioner {
  public TuanSimplePartitioner(VerifiableProperties props) {}

  public int partition(Object key, int a_numPartitions) {
    String strKey = (String) key;
    String subKey = strKey;
    int partition = 0;
    int offset = strKey.lastIndexOf('.');
    if (offset > 0) {
      subKey = strKey.substring(offset + 1);
    }
    partition = Math.abs(subKey.hashCode() % a_numPartitions);
    // Or simple:
    // partition = Math.abs(strKey.hashCode()) % a_numPartitions;
    return partition;
  }
}
