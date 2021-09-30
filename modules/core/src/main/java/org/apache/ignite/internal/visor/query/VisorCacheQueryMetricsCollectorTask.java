package org.apache.ignite.internal.visor.query;

import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Task that collect cache query metrics (see {@link VisorQueryMetrics}) from all nodes.
 */
@GridInternal
public class VisorCacheQueryMetricsCollectorTask extends VisorMultiNodeTask<VisorCacheQueryMetricsCollectorTaskArg,
        Iterable<VisorCacheQueryAggMetrics>, Map<String, VisorQueryMetrics>> {
  /** */
  private static final long serialVersionUID = 0L;

  @Override
  protected VisorCacheQueryMetricsCollectorJob job(VisorCacheQueryMetricsCollectorTaskArg arg) {
    return new VisorCacheQueryMetricsCollectorJob(arg, debug);
  }

  @Override
  protected @Nullable Iterable<VisorCacheQueryAggMetrics> reduce0(List<ComputeJobResult> results) throws IgniteException {
    Map<String, VisorCacheQueryAggMetrics> grpAggrMetrics = U.newHashMap(results.size());

    for (ComputeJobResult res : results) {
      if (res.getException() == null) {
        Map<String, VisorQueryMetrics> cms = res.getData();

        for (Map.Entry<String, VisorQueryMetrics> cm : cms.entrySet()) {
          VisorCacheQueryAggMetrics am = grpAggrMetrics.computeIfAbsent(cm.getKey(), VisorCacheQueryAggMetrics::new);
          am.getNodes().put(res.getNode().id(), cm.getValue());
        }
      }
    }

    // Create serializable result.
    return new ArrayList<>(grpAggrMetrics.values());
  }
}
