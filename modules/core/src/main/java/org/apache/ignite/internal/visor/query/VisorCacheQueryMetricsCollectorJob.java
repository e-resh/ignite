package org.apache.ignite.internal.visor.query;

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isIgfsCache;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;

public class VisorCacheQueryMetricsCollectorJob extends VisorJob<VisorCacheQueryMetricsCollectorTaskArg, Map<String, VisorQueryMetrics>> {
  /** */
  private static final long serialVersionUID = 0L;

  /**
   * Create job with given argument.
   *
   * @param arg Whether to collect metrics for all caches or for specified cache name only.
   * @param debug Debug flag.
   */
  VisorCacheQueryMetricsCollectorJob(VisorCacheQueryMetricsCollectorTaskArg arg, boolean debug) {
    super(arg, debug);
  }

  @Override
  protected Map<String, VisorQueryMetrics> run(@Nullable VisorCacheQueryMetricsCollectorTaskArg arg) throws IgniteException {
    assert arg != null;

    Collection<String> cacheNames = arg.getCacheNames() != null ? arg.getCacheNames() : new ArrayList<>();

    assert cacheNames != null;

    IgniteConfiguration cfg = ignite.configuration();

    GridCacheProcessor cacheProcessor = ignite.context().cache();

    Collection<IgniteCacheProxy<?, ?>> caches = cacheProcessor.jcaches();

    Map<String, VisorQueryMetrics> res = new HashMap<>(caches.size());

    boolean allCaches = cacheNames.isEmpty();

    for (IgniteCacheProxy<?, ?> ca : caches) {
      String cacheName = ca.getName();

      if ((allCaches || cacheNames.contains(cacheName)) && !VisorTaskUtils.isRestartingCache(ignite, cacheName)) {
        GridCacheContext<?,?> ctx = ca.context();

        if (ctx.started() && !isSystemCache(cacheName) && !isIgfsCache(cfg, cacheName)) {
          VisorQueryMetrics m = new VisorQueryMetrics(ca.queryMetrics());
          res.put(cacheName, m);
        }
      }
    }

    return res;
  }

  /** {@inheritDoc} */
  @Override public String toString() {
    return S.toString(VisorCacheQueryMetricsCollectorJob.class, this);
  }
}
