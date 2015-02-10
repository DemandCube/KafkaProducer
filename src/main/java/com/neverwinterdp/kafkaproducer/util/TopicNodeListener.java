package com.neverwinterdp.kafkaproducer.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

public class TopicNodeListener implements PathChildrenCacheListener {

  private String topic;

  public TopicNodeListener(String topic) {
    super();
    this.topic = topic;
  }

  public String getTopic() {
    return topic;
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
    System.out.println(event.getData());
  }
}
