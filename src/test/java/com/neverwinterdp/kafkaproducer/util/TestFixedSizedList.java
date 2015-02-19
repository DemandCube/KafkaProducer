package com.neverwinterdp.kafkaproducer.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class TestFixedSizedList {

  int size = 10;
  FixedSizeList<Integer> list;

  @Before
  public void setup() {
    list = new FixedSizeList<>(size);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAdd() {

    for (int i = 0; i < size; i++) {
      assertTrue(list.add(i));
    }

    list.clear();
    size = 11;
    for (int i = 0; i < size; i++) {
      assertTrue(list.add(i));
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAddIndex() {
    for (int i = 0; i < size; i++) {
      list.add(i, i);
    }

    list.clear();
    size = 11;
    for (int i = 0; i < size; i++) {
      list.add(i, i);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAddAll() {
    List<Integer> ints = new ArrayList<Integer>();

    for (int i = 0; i < size; i++) {
      ints.add(i);
    }
    assertTrue(list.addAll(ints));

    list.clear();
    size = 1;
    for (int i = 0; i < size; i++) {
      ints.add(i);
    }
    assertTrue(list.addAll(ints));
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAddAllIndex() {
    List<Integer> ints = new ArrayList<Integer>();

    for (int i = 0; i < size; i++) {
      ints.add(i);
    }
    assertTrue(list.addAll(0, ints));

    list.clear();
    size = 1;
    for (int i = 0; i < size; i++) {
      ints.add(i);
    }
    assertTrue(list.addAll(0, ints));
  }
}
