package com.neverwinterdp.kafka.producer.util;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

class Topic {
  int version;
  Map<String, Set<Integer>> partitions;


  public Topic() {
    super();
    partitions = Collections.emptyMap();
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public Map<String, Set<Integer>> getPartitions() {
    return partitions;
  }

  public void setPartitions(Map<String, Set<Integer>> partitions) {
    this.partitions = partitions;
  }
}