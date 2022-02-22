package org.apache.ignite.internal.binary.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.util.UnsafeUtils;
import org.apache.ignite.binary.Compressor;
import org.apache.ignite.configuration.CompressionConfiguration;

public class LZ4CompressAdapter implements Compressor {

  static final LZ4Factory factory = LZ4Factory.fastestInstance();

  static final LZ4FastDecompressor decompressor = factory.fastDecompressor();

  static final LZ4Compressor fastCompressor = factory.fastCompressor();

  private LZ4Compressor compressor;

  @Override
  public void configure(CompressionConfiguration compressionCfg) {
    int level = compressionCfg.getCompressionLevel();
    assert level >= 0 && level <= 17: level;
    compressor = level == 0 ? fastCompressor : factory.highCompressor(level);
  }

  @Override
  public byte[] tryCompress(byte[] source) {
    assert compressor != null : "Compressor is not ready";

    byte[] compressed = compressor.compress(source); // Compress source data
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
