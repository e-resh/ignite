package org.apache.ignite.internal.binary.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.util.UnsafeUtils;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.Compressor;
import org.apache.ignite.configuration.CompressionConfiguration;
import org.apache.ignite.internal.GridKernalContext;

public class LZ4CompressAdapter implements Compressor {
  private static  final long LOG_THROTTLE_STEP = 100_000;

  static final LZ4Factory factory = LZ4Factory.fastestInstance();

  static final LZ4FastDecompressor decompressor = factory.fastDecompressor();

  static final LZ4Compressor fastCompressor = factory.fastCompressor();

  private final ThreadLocal<Long> logThrottleCntr = ThreadLocal.withInitial(() -> 0L);

  private LZ4Compressor compressor;

  private IgniteLogger log;

  private String cacheName;

  @Override
  public void configure(GridKernalContext ctx, String cacheName, CompressionConfiguration compressionCfg) {
    int level = compressionCfg.getCompressionLevel();
    assert level >= 0 && level <= 17: level;

    this.cacheName = cacheName;

    log = ctx.log(LZ4CompressAdapter.class);

    compressor = level == 0 ? fastCompressor : factory.highCompressor(level);

    log.info("Cache " + cacheName + ": LZ4 compression with level: " + level);
  }

  @Override
  public byte[] tryCompress(byte[] source) {
    assert log != null : "Logger is not ready";
    assert compressor != null : "Compressor is not ready";

    byte[] compressed = compressor.compress(source); // Compress source data

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

    byte[] result = new byte[compressed.length + 4]; // Create result compressed array
    UnsafeUtils.writeInt(result, 0, source.length); // Add to start source data array length
    System.arraycopy(compressed, 0, result, 4, compressed.length); // Copy compressed data with Integer offset
    return result;
  }

  @Override
  public byte[] decompress(byte[] compressed) {
    int destLength = SafeUtils.readInt(compressed, 0);
    return decompressor.decompress(compressed, 4, destLength);
  }
}
