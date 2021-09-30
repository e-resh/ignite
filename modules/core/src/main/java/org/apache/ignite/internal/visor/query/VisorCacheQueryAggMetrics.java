package org.apache.ignite.internal.visor.query;

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Data transfer object for cache query aggregation metrics
 */
public class VisorCacheQueryAggMetrics extends IgniteDataTransferObject {
  /** */
  private static final long serialVersionUID = 0L;

  /** Cache name. */
  private String name;

  /** Node IDs with cache metrics. */
  private Map<UUID, VisorQueryMetrics> metrics = new HashMap<>();

  /** Minimum execution time of query. */
  private transient Long minQryTime;

  /** Average execution time of query. */
  private transient Double avgQryTime;

  /** Maximum execution time of query. */
  private transient Long maxQryTime;

  /** Total execution time of query. */
  private transient Long totalQryTime;

  /** Number of executions. */
  private transient Integer execsQry;

  /** Total number of times a query execution failed. */
  private transient Integer failsQry;

  /**
   * Default constructor.
   */
  public VisorCacheQueryAggMetrics() {
    // No-op.
  }

  /**
   * Create data transfer object for aggregated cache query metrics.
   *
   * @param cacheName cache name
   */
  public VisorCacheQueryAggMetrics(String cacheName) {
    name = cacheName;
  }

  /**
   * @return Cache name.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Map of Node IDs to cache query metrics.
   */
  public Map<UUID, VisorQueryMetrics> getNodes() {
    return metrics;
  }

  /**
   * @return Minimum execution time of query.
   */
  public long getMinimumQueryTime() {
    if (minQryTime == null) {
      minQryTime = Long.MAX_VALUE;

      for (VisorQueryMetrics metric : metrics.values())
        minQryTime = Math.min(minQryTime, metric.getMinimumTime());
    }

    return minQryTime;
  }

  /**
   * @return Average execution time of query.
   */
  public double getAverageQueryTime() {
    if (avgQryTime == null) {
      avgQryTime = 0.0d;

      for (VisorQueryMetrics metric : metrics.values())
        avgQryTime += metric.getAverageTime();

      avgQryTime /= metrics.size();
    }

    return avgQryTime;
  }

  /**
   * @return Maximum execution time of query.
   */
  public long getMaximumQueryTime() {
    if (maxQryTime == null) {
      maxQryTime = Long.MIN_VALUE;

      for (VisorQueryMetrics metric : metrics.values())
        maxQryTime = Math.max(maxQryTime, metric.getMaximumTime());
    }

    return maxQryTime;
  }

  /**
   * @return Total execution time of query.
   */
  public long getTotalQueryTime() {
    if (totalQryTime == null)
      totalQryTime = (long)(getAverageQueryTime() * getQueryExecutions());

    return totalQryTime;
  }

  /**
   * @return Number of executions.
   */
  public int getQueryExecutions() {
    if (execsQry == null) {
      execsQry = 0;

      for (VisorQueryMetrics metric : metrics.values())
        execsQry += metric.getExecutions();
    }

    return execsQry;
  }

  /**
   * @return Total number of times a query execution failed.
   */
  public int getQueryFailures() {
    if (failsQry == null) {
      failsQry = 0;

      for (VisorQueryMetrics metric : metrics.values())
        failsQry += metric.getFailures();
    }

    return failsQry;
  }


  @Override
  protected void writeExternalData(ObjectOutput out) throws IOException {
    U.writeString(out, name);
    U.writeMap(out, metrics);
    out.writeObject(minQryTime);
    out.writeObject(avgQryTime);
    out.writeObject(maxQryTime);
    out.writeObject(totalQryTime);
    out.writeObject(execsQry);
    out.writeObject(failsQry);
  }

  @Override
  protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
    name = U.readString(in);
    metrics = U.readMap(in);
    minQryTime = (Long)in.readObject();
    avgQryTime = (Double)in.readObject();
    maxQryTime = (Long)in.readObject();
    totalQryTime = (Long)in.readObject();
    execsQry = (Integer)in.readObject();
    failsQry = (Integer)in.readObject();
  }

  /** {@inheritDoc} */
  @Override public String toString() {
    return S.toString(VisorCacheQueryAggMetrics.class, this);
  }
}
