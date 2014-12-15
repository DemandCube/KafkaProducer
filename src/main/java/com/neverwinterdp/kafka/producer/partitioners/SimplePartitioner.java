package com.neverwinterdp.kafka.producer.partitioners;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * A partitioner that writes to the specified partition.
 * 
 * The required partition is passed to the Partitioner as a string via 
 * 
 * <code> KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic,partition,message);</code>
 * */
public class SimplePartitioner implements Partitioner {

  public SimplePartitioner(VerifiableProperties props) {}

  @Override
  public int partition(Object key, int numPartitions) {
    String keyString = (String) key;

    //between "PARTITION:" and ","
    String partitionString = keyString.substring(keyString.indexOf("PARTITION:") + 1, keyString.indexOf(","));
    int partition = Integer.parseInt(partitionString);
    //System.out.println("HUHUHU " + partition);
    return partition <= numPartitions ? partition : numPartitions;
  }
}
