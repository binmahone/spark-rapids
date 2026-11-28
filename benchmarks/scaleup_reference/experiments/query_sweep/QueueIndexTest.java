/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import com.nvidia.spark.rapids.HashedPriorityQueue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Differential state transitions and a falsifiable hash-work bound for heap sifting. */
public final class QueueIndexTest {
  private static long hashes;
  private static final class Item {
    final int id;
    int priority;
    Item(int id, int priority) { this.id = id; this.priority = priority; }
    @Override public int hashCode() { hashes++; return id % 17; }
    @Override public boolean equals(Object other) { return other instanceof Item && ((Item) other).id == id; }
  }
  private static final Comparator<Item> ORDER = Comparator.comparingInt((Item x) -> x.priority)
      .thenComparingInt(x -> x.id);
  private static void check(boolean condition) {
    if (!condition) { throw new AssertionError("Queue state differs"); }
  }
  public static void main(String[] args) {
    HashedPriorityQueue<Item> queue = new HashedPriorityQueue<>(2, ORDER);
    Map<Integer, Item> reference = new HashMap<>();
    Random random = new Random(1776);
    for (int step = 0; step < 10000; step++) {
      int id = random.nextInt(256);
      Item item = new Item(id, random.nextInt(1000));
      switch (random.nextInt(6)) {
        case 0:
          check(queue.offer(item) == (reference.putIfAbsent(id, item) == null));
          break;
        case 1:
          check(queue.remove(item) == (reference.remove(id) != null));
          break;
        case 2:
          List<Item> sorted = new ArrayList<>(reference.values());
          sorted.sort(ORDER);
          Item expected = sorted.isEmpty() ? null : sorted.get(0);
          check(queue.poll() == expected);
          if (expected != null) { reference.remove(expected.id); }
          break;
        case 3:
          Item old = reference.get(id);
          if (old != null) { old.priority = item.priority; queue.priorityUpdated(old); }
          break;
        case 4:
          List<Item> ordered = new ArrayList<>(reference.values());
          ordered.sort(ORDER);
          Iterator<Item> iterator = queue.priorityIterator();
          for (Item x : ordered) { check(iterator.hasNext() && iterator.next() == x); }
          check(!iterator.hasNext());
          break;
        default:
          if (step % 127 == 0) { queue.clear(); reference.clear(); }
          break;
      }
      check(queue.size() == reference.size());
      for (Item x : reference.values()) { check(queue.contains(new Item(x.id, -1))); }
    }
    queue.clear();
    int n = 4096;
    for (int i = n - 1; i >= 0; i--) { check(queue.offer(new Item(i, i))); }
    hashes = 0;
    long start = System.nanoTime();
    for (int i = 0; i < n; i++) { check(queue.poll().id == i); }
    long elapsed = System.nanoTime() - start;
    check(queue.isEmpty());
    System.out.println("QUEUE_INDEX_TEST hashes=" + hashes + " elements=" + n + " drain_ns=" + elapsed);
    if (args.length == 1 && args[0].equals("bounded")) { check(hashes <= 2L * n); }
  }
}
