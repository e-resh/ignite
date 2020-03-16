package org.apache.ignite.yardstick.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.yardstick.cache.model.Person8;
import org.yardstickframework.BenchmarkConfiguration;

import java.util.Map;

import static org.yardstickframework.BenchmarkUtils.println;

public class IgniteDataPutLoadIndexedValue8Benchmark extends IgniteCacheAbstractBenchmark<Integer, Person8> {
  private static final int DEF_NODE_BUFF_SIZE = 64;

  @Override
  public void setUp(BenchmarkConfiguration cfg) throws Exception {
    super.setUp(cfg);

    try (IgniteDataStreamer<Integer, Person8> dataLdr = streamer()) {
      for (int i = 0; i < args.range() && !Thread.currentThread().isInterrupted();) {
        dataLdr.addData(i, new Person8(i));

        if (++i % 10_000 == 0)
          println(cfg, "Items populated: " + i);
      }
    }
    println(cfg, "Populated " + args.range() + " items");
  }

  /** {@inheritDoc} */
  @Override public boolean test(Map<Object, Object> ctx) throws Exception {
    IgniteCache<Integer, Person8> cache = cacheForOperation();
    int key = nextRandom(args.range());
    cache.put(key, new Person8(key));
    return true;
  }

  /** {@inheritDoc} */
  @Override protected IgniteCache<Integer, Person8> cache() {
    return ignite().cache("atomic-index");
  }

  private IgniteDataStreamer<Integer, Person8> streamer() {
    IgniteCache<Integer, Person8> cache = cacheForOperation();
    IgniteDataStreamer<Integer, Person8> streamer = ignite().dataStreamer(cache.getName());

    streamer.allowOverwrite(true);
    streamer.receiver(DataStreamerCacheUpdaters.batched());

    streamer.perThreadBufferSize(DEF_NODE_BUFF_SIZE);
    streamer.perNodeBufferSize(DEF_NODE_BUFF_SIZE);

    streamer.perNodeParallelOperations(1);

    return streamer;
  }
}
