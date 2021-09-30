package org.apache.ignite.internal.visor.query;

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Arguments for task {@link VisorCacheQueryMetricsCollectorTask}
 */
public class VisorCacheQueryMetricsCollectorTaskArg extends IgniteDataTransferObject {
  /** */
  private static final long serialVersionUID = 0L;

  /** Cache names to collect metrics. */
  private List<String> cacheNames;

  /**
   * Default constructor.
   */
  public VisorCacheQueryMetricsCollectorTaskArg() {
    // No-op.
  }

  /**
   * @param cacheNames Cache names to collect metrics.
   */
  public VisorCacheQueryMetricsCollectorTaskArg(List<String> cacheNames) {
    this.cacheNames = cacheNames;
  }

  /**
   * @return Cache names to collect metrics
   */
  public List<String> getCacheNames() {
    return cacheNames;
  }

  /** {@inheritDoc} */
  @Override protected void writeExternalData(ObjectOutput out) throws IOException {
    U.writeCollection(out, cacheNames);
  }

  /** {@inheritDoc} */
  @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
    cacheNames = U.readList(in);
  }

  /** {@inheritDoc} */
  @Override public String toString() {
    return S.toString(VisorCacheQueryMetricsCollectorTaskArg.class, this);
  }
}
