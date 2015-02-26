package com.neverwinterdp.kafkaproducer.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ForwardingList;

//TODO use a LinkedBlockingQueue with an exception thrown when offer returns false
public class FixedSizeList<E> extends ForwardingList<E> {

  private LinkedList<E> delegate = new LinkedList<E>();
  private int maxSize;

  public FixedSizeList(int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  protected List<E> delegate() {
    return delegate;
  }

  @Override
  public void add(int index, E elem) {
    if (index >= maxSize) {
      throw new IndexOutOfBoundsException("Unable to add");
    }
    super.add(index, elem);
  }

  @Override
  public boolean add(E elem) {
    if (delegate.size() >= maxSize) {
      throw new IndexOutOfBoundsException("Unable to add");
    }
    return standardAdd(elem);

  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    if (delegate.size() + c.size() >= maxSize) {
      throw new IndexOutOfBoundsException("Unable to add");
    }
    return standardAddAll(c);
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    if (delegate.size() + c.size() >= maxSize) {
      throw new IndexOutOfBoundsException("Unable to add");
    }
    return standardAddAll(index, c);
  }
}
