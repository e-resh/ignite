package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class PageTypeOperationsTracker {
  private final ConcurrentHashMap<Integer, Long> tracker = new ConcurrentHashMap<>();

  private final AtomicLong counter = new AtomicLong(0);

  void trackPage(int type) {
    tracker.compute(type, (t, processed) -> processed != null ? processed + 1 : 1);
    counter.getAndIncrement();
  }

  Long tracked() {
    return counter.get();
  }

  TreeMap<Integer, Long> trackedPageValues() {
    return new TreeMap<>(tracker);
  }
}
