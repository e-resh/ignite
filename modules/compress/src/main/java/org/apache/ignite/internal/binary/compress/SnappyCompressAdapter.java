package org.apache.ignite.internal.binary.compress;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.Compressor;
import org.apache.ignite.configuration.CompressionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.Arrays;

public class SnappyCompressAdapter implements Compressor {
  private static  final long LOG_THROTTLE_STEP = 10_000;

  private final ThreadLocal<Long> logThrottleCntr = ThreadLocal.withInitial(() -> 0L);

  private IgniteLogger log;

  private String cacheName;

  @Override
  public void configure(GridKernalContext ctx, String cacheName, CompressionConfiguration compressionCfg) {
    this.cacheName = cacheName;

    log = ctx.log(SnappyCompressAdapter.class);
    log.info("Cache " + cacheName + ": Snappy compression");
  }

  @Override
  public byte[] tryCompress(byte[] source) {
    assert log != null : "Logger is not ready";
    assert cacheName != null : "Unknown cache name";

    try {
      byte[] compressed = Snappy.compress(source);

      if (log.isDebugEnabled()) {
        if (compressed.length >= source.length) {
          long cntr = logThrottleCntr.get();
          if (cntr % LOG_THROTTLE_STEP == 0) {
            log.warning("Cache name: " + cacheName +
                    " compression is not efficient (source length = " + source.length +
                    ", compressed length = " + compressed.length + ")");
          }
          logThrottleCntr.set(++cntr);
        }
      }
      return compressed;
    } catch (IOException e) {
      throw new IgniteException("Impossible uncompress cache " + cacheName + " bytes " + Arrays.toString(source));
    }
  }

  @Override
  public byte[] decompress(byte[] bytes) {
    assert cacheName != null : "Unknown cache name";

    try {
      return Snappy.uncompress(bytes);
    } catch (IOException e) {
      throw new IgniteException("Impossible uncompress cache " + cacheName + " bytes " + Arrays.toString(bytes));
    }
  }
}
