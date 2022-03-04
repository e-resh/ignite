package org.apache.ignite.internal.binary.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.util.UnsafeUtils;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.Compressor;
import org.apache.ignite.configuration.CompressionConfiguration;
import org.apache.ignite.internal.GridKernalContext;

import java.util.Arrays;

public class LZ4CompressAdapter implements Compressor {
  private static  final long LOG_THROTTLE_STEP = 100_000;

  private static final byte BUF_LEN_MASK = 0b00001111;

  static final LZ4Factory factory = LZ4Factory.fastestInstance();

  static final LZ4SafeDecompressor decompressor = factory.safeDecompressor();

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

    byte header = buildHeader(source.length, compressed.length); // Calculate header

    byte[] result = new byte[compressed.length + 1]; // Create result compressed array
    UnsafeUtils.writeByte(result, 0, header); // Add to start source data array length
    System.arraycopy(compressed, 0, result, 1, compressed.length); // Copy compressed data with Integer offset
    return result;
  }

  @Override
  public byte[] decompress(byte[] compressed) {
    byte header = SafeUtils.readByte(compressed, 0);
    int destLengthBuffer = estimateBufferLength(header, compressed.length);

    byte[] buffer = new byte[destLengthBuffer];
    int size = decompressor.decompress(compressed, 1, compressed.length-1, buffer, 0);

    return size != destLengthBuffer ? Arrays.copyOfRange(buffer, 0, size) : buffer;
  }

  // First byte of compressed array is header byte, to specify dictionary
  // and approximate buffer size needed to decompress.
  // The format is 0b0000XXXX
  // 0bXXXX0000 - reserved
  //
  // Mapping: 0000 - compressed length * 1
  //          0001 - compressed length * 2
  //          0010 - compressed length * 4
  //          0011 - compressed length * 8
  //          ...
  //          1111 - compressed length * 32768
  private static byte buildHeader(int inputSize, int compressedSize) {
    if (inputSize > compressedSize) {
      byte header = 0;
      int ratio = inputSize / compressedSize;
      for (int i = 1; i < 16; i++) {
        if (ratio < 1 << i) {
          header |= i;
          break;
        }
      }
      return header;
    } else {
      return 0;
    }
  }

  private static int estimateBufferLength(byte header, int compressedSize) {
    int bufLenHint = header & BUF_LEN_MASK;
    return bufLenHint > 0 ? compressedSize << bufLenHint : compressedSize;
  }
}
