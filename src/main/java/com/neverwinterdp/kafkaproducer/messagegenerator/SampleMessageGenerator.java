package com.neverwinterdp.kafkaproducer.messagegenerator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import kafka.producer.Partitioner;

import com.neverwinterdp.kafkaproducer.partitioner.SimplePartitioner;

// TOPIC:3fdd6c2047c4e61e, PARTITION:0, WriterID:1, SEQUENCE:100, TIME:10:04:49:0174
// PARTITION:0, WriterID:1, SEQUENCE:100

/**
 * A message producer for the kafka writer. Alternate implementations of this class will have a
 * blocking queue that calls to next() poll. This will allow assynchronous producers of data to put
 * in to the blocking queue as well.
 * 
 * Note that a message generator is responsible for defining its partitioner
 * */
public class SampleMessageGenerator implements MessageGenerator<String> {

  private AtomicLong sequenceID;
  String message;
  Date now;
  private String topic;
  private int writerId;
  private int partition;
  private Class<? extends Partitioner> partitionerClass;

  public SampleMessageGenerator(String topic, int partition, int id) {
    super();
    this.topic = topic;
    this.partition = partition;
    this.writerId = id;
    sequenceID = new AtomicLong(0);
    this.partitionerClass = SimplePartitioner.class;
  }

  @Override
  public boolean hasNext() {
    return sequenceID.get() < Long.MAX_VALUE;
  }

  @Override
  public String next() {
    now = new Date();
    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSSS");
    message =
        "TOPIC:" + topic + ", PARTITION:" + partition + ", WriterID:" + writerId + ", SEQUENCE:"
            + sequenceID.incrementAndGet() + ", TIME:" + dateFormat.format(now);
    return message;
  }

  @Override
  public void remove() {
    sequenceID.decrementAndGet();
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
