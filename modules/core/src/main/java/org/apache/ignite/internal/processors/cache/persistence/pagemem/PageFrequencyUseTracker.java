package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;

import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

class PageFrequencyUseTracker {
  private final IgniteLogger log;

  private final ConcurrentHashMap<FullPageId, PageUsageInfo> tracker = new ConcurrentHashMap<>();

  private final AtomicLong counter = new AtomicLong(0);

  PageFrequencyUseTracker(IgniteLogger log) {
    this.log = log;
  }

  void trackPage(FullPageId pageId, int type) {
    tracker.compute(pageId, (id, usage) -> updatePageInfo(id, usage, type));
    counter.getAndIncrement();
  }

  private PageUsageInfo updatePageInfo(FullPageId pageId, PageUsageInfo old, int type) {
    PageUsageInfo newValue = old != null ? old.inc() : new PageUsageInfo();
    int init = newValue.getType();
    if (init > 0 && type > 0 && type != init) {
      log.warning("Different page (" + pageId + ") types [ init:" + init + ", new:" + type + " ]");
    }
    return type > 0 ? newValue.type(type) : newValue;
  }

  Long tracked() {
    return counter.get();
  }

  TreeMap<Integer, Long> trackedPageValues(int boundary) {
    return new TreeMap<>(tracker.values().stream()
            .filter(info -> info.getLoaded() > boundary)
            .collect(Collectors.groupingBy(PageUsageInfo::getType, Collectors.summingLong(PageUsageInfo::getLoaded))));
  }

  static class PageUsageInfo {
    int type = 0;
    long loaded = 1;

    PageUsageInfo inc() {
      this.loaded++;
      return this;
    }

    PageUsageInfo type(int type) {
      this.type = type;
      return this;
    }

    public int getType() {
      return type;
    }

    long getLoaded() {
      return loaded;
    }
  }
}
