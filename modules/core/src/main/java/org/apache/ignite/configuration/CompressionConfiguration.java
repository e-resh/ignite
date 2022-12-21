package org.apache.ignite.configuration;

import java.io.Serializable;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.Compressor;

public class CompressionConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Compression enabled. */
    private boolean enabled;

    /**
     * Compression level. {@code 0} to use default.
     * ZSTD: from {@code -131072} to {@code 22} (default {@code 3}).
     * LZ4: from {@code 0} to {@code 17} (default {@code 0}).
     */
    private int compressionLevel;

    /**
     * Compression algorithm.
     * Supported: LZ4, ZSTD or Snappy
     */
    private BinaryObjectCompressionAlgorithm compressionAlgorithm = BinaryObjectCompressionAlgorithm.LZ4;

    /** Compressor factory. */
    private Factory<? extends Compressor> compressorFactory = new CompressorFactory();

    /**
     * Gets compression enabled flag.
     * By default {@code true} (use {@code null} CompressionConfiguration as disabled default).
     *
     * @return compression enabled flag.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets compression enabled flag.
     *
     * @param enabled Compression enabled flag.
     */
    public CompressionConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }


    /**
     * Gets compression level, as interpreted by {@link Compressor} implementation.
     *
     * @return compression level.
     */
    public int getCompressionLevel() {
        return compressionLevel;
    }

    /**
     * Sets compression level, as interpreted by {@link Compressor} implementation.
     *
     * It is recommended to keep default value or do extensive benchmarking after changing this setting.
     *
     * Note that default is chosen for {@code zstd-jni} library.
     *
     * @param compressionLevel compression level.
     */
    public CompressionConfiguration setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;

        return this;
    }

    /**
     * Gets compression-decompression implementation to use.
     *
     * By default, {@code ignite-compress} is used which employs {@code zstd-jni} library.
     *
     * @return compressor factory.
     */
    public Factory<? extends Compressor> getCompressorFactory() {
        return compressorFactory;
    }

    /**
     * Sets compression-decompression implementation to use.
     *
     * Note that custom {@link Compressor} implementation may use a subset of configuration settings
     * and-or use their own settings provided by {@link #compressorFactory}.
     *
     * @param compressorFactory compressor factory.
     */
    public CompressionConfiguration setCompressorFactory(Factory<? extends Compressor> compressorFactory) {
        this.compressorFactory = compressorFactory;

        return this;
    }

    /**
     * Gets binary object compression algorithm.
     * Makes sense only with enabled {@link DataRegionConfiguration#setPersistenceEnabled persistence}.
     *
     * @return Binary object compression algorithm.
     * @see #getCompressionLevel
     */
    public BinaryObjectCompressionAlgorithm getCompressionAlgorithm() {
        return compressionAlgorithm;
    }

    /**
     * Sets binary object compression algorithm.
     * Makes sense only with enabled {@link DataRegionConfiguration#setPersistenceEnabled persistence}.
     *
     * @param compressionAlgorithm Binary object compression algorithm.
     * @return {@code this} for chaining.
     * @see #setCompressionLevel
     */
    public CompressionConfiguration setCompressionAlgorithm(BinaryObjectCompressionAlgorithm compressionAlgorithm) {
        this.compressionAlgorithm = compressionAlgorithm;

        return this;
    }

    /** */
    private class CompressorFactory implements Factory<Compressor> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @Override public Compressor create() {
            Compressor res;
            String compressorAdapterClass = getCompressorClass();
            try {
                Class<? extends Compressor> cls = (Class<? extends Compressor>) Class.forName(compressorAdapterClass);
                res = cls.newInstance();
            } catch (Throwable t) {
                throw new IgniteException("Failed to instantiate Compressor for class: " + compressorAdapterClass +
                    " (please ensure that \"ignite-compress\" module is added to classpath)." , t);
            }
            return res;
        }

        private String getCompressorClass() {
            BinaryObjectCompressionAlgorithm alg = getCompressionAlgorithm();
            switch (alg) {
                case LZ4:
                    return "org.apache.ignite.internal.binary.compress.LZ4CompressAdapter";
                case SNAPPY:
                    return "org.apache.ignite.internal.binary.compress.SnappyCompressAdapter";
                case ZSTD:
                    return "org.apache.ignite.internal.binary.compress.ZstdCompressAdapter";
                default:
                    throw new IllegalStateException("Unsupported compression algorithm: " + alg);
            }
        }
    }
}
