package org.apache.ignite.internal.binary.compress;

import com.github.luben.zstd.Zstd;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.Compressor;
import org.apache.ignite.configuration.CompressionConfiguration;
import org.apache.ignite.internal.GridKernalContext;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ZSTD: from {@code -131072} to {@code 22} (default {@code 3})
 */
public class ZstdCompressAdapter implements Compressor {
  private final long PRINT_PER = 65536;

  /** */
  private final boolean CHECK = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_COMPRESSION_CHECK);

  protected static final int MIN_DELTA_BYTES = 8;

  protected final AtomicLong totalSamples = new AtomicLong();
  protected final AtomicLong acceptedSamples = new AtomicLong();

  protected final AtomicLong totalUncompressedSz = new AtomicLong();
  protected final AtomicLong totalCompressedSz = new AtomicLong();

  private static  final long LOG_THROTTLE_STEP = 10_000;

  private final ThreadLocal<Long> logThrottleCntr = ThreadLocal.withInitial(() -> 0L);

  private volatile int level;

  private IgniteLogger log;

  private String cacheName;

  @Override
  public void configure(GridKernalContext ctx, String cacheName, CompressionConfiguration compressionCfg) {
    int level = compressionCfg.getCompressionLevel();
    assert level >= -131072 && level <= 22: level;

    this.level = level;
    this.cacheName = cacheName;

    log = ctx.log(ZstdCompressAdapter.class);
    log.info("Cache " + cacheName + ": LSTD stateless compression with level: " + level);
  }

  @Override public byte[] tryCompress(byte[] input) {
    byte[] compressed = compress(input, level);
    return appraiseAndAddHeader(input, compressed, 1);
  }

  public static byte[] compress(byte[] src, int level) {
    long maxDstSize = Zstd.compressBound(src.length);

    if (maxDstSize > Integer.MAX_VALUE) {
      throw new RuntimeException("Max output size is greater than MAX_INT");
    }

    byte[] dst = new byte[(int) maxDstSize + 1];
    long size = Zstd.compressByteArray(dst, 1, dst.length - 1, src, 0, src.length, level);

    if (Zstd.isError(size)) {
      throw new RuntimeException(Zstd.getErrorName(size));
    }

    return Arrays.copyOfRange(dst, 0, (int) size + 1);
  }

  protected byte[] appraiseAndAddHeader(byte[] input, byte[] compressed, int dict) {
    int inputLen = input.length;
    if (compressed != null) {
      byte header = 0;
      long len = compressed.length;

      if (len <= inputLen / 2048) {
        byte[] newCompressed = new byte[inputLen / 2048];
        System.arraycopy(compressed, 0, newCompressed, 0, compressed.length);
        compressed = newCompressed;
        header |= 3 << BUF_LEN_SHIFT;
      }
      else if (len <= inputLen / 256)
        header |= 3 << BUF_LEN_SHIFT;
      else if (len <= inputLen / 32)
        header |= 2 << BUF_LEN_SHIFT;
      else if (len <= inputLen / 4)
        header |= 1 << BUF_LEN_SHIFT;

      assert (dict & ~DICT_MASK) == 0;

      header |= dict;

      compressed[0] = header;

      if (CHECK) {
        byte[] decompressed = decompress(compressed);

        if (!Arrays.equals(input, decompressed)) {
          throw new IgniteException("Recompressed did not match: [input=" +
                  Arrays.toString(input) + ", afterDecompress=" + Arrays.toString(decompressed) + "]");
        }
      }
    }


    byte[] message = compressed == null ? input : compressed;

    totalUncompressedSz.addAndGet(inputLen);
    totalCompressedSz.addAndGet(message.length);

    if (totalSamples.incrementAndGet() % PRINT_PER == 0L)
      System.out.println("Ratio: " + (float)totalCompressedSz.get() / (float)totalUncompressedSz.get() +
              ", acceptance: " + (acceptedSamples.get() * 100L) / (totalSamples.get()) + "%");

    if ((inputLen - message.length) > MIN_DELTA_BYTES) {
      acceptedSamples.incrementAndGet();
      return compressed;
    }

    return null;
  }

  // First byte of compressed array is header byte, to specify dictionary
  // and approximate buffer size needed to decompress.
  // The format is 0b0BBDDDDD
  // 0x1XXXXXXX - reserved for future use.
  // 0x0XX00000 - reserved
  //
  // BB mapping: 00 - compressed length * 4
  //             01 - compressed length * 32
  //             10 - compressed length * 256
  //             11 - compressed length * 2048
  // (This is important since cleaning buffers takes CPU time. >2048x isn't supported)
  //
  // DDDDD mapping:
  //             0 - reserved
  //             1 ~ 30 - dictionaries in rotation
  //             31 - no dictionary

  private static final byte BUF_LEN_MASK = 0b01100000;
  private static final byte BUF_LEN_SHIFT = 5;
  private static final byte DICT_MASK = 0b00011111;

  @Override
  public byte[] decompress(byte[] bytes) {
    byte header = bytes[0];

    if (header <= 0)
      throw new IgniteException("Unknown compression header value: " + header);

    int bufLenHint = header & BUF_LEN_MASK;
    int maxLength;

    if (bufLenHint == 0)
      maxLength = bytes.length * 4;
    else {
      if (bufLenHint == 1 << BUF_LEN_SHIFT)
        maxLength = bytes.length * 32;
      else if (bufLenHint == 2 << BUF_LEN_SHIFT)
        maxLength = bytes.length * 256;
      else if (bufLenHint == 3 << BUF_LEN_SHIFT)
        maxLength = bytes.length * 2048;
      else
        throw new IgniteException("Impossible buffer length shift: " + bufLenHint);
    }

    assert maxLength > 0;

    return decompress(bytes, maxLength);
  }

  public static byte[] decompress(byte[] src, int originalSize) {
    byte[] dst = new byte[originalSize];

    long size = Zstd.decompressByteArray(dst, 0, dst.length, src, 1, src.length - 1);

    if (Zstd.isError(size))
      throw new RuntimeException(Zstd.getErrorName(size));

    if (size != originalSize)
      return Arrays.copyOfRange(dst, 0, (int) size);
    else
      return dst;
  }
}
