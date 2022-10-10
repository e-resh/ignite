package org.apache.ignite.internal.binary;

import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.io.File;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

public class BinaryMetaDataCacheEvictionTest extends GridCommonAbstractTest {

  public static final String CACHE_NAME_1 = "PAYMENT_TEST1", CACHE_NAME_2 = "PAYMENT_TEST2";

  public static final String CACHE_VAL_TYPE = "PaymentEntity";

  public static final String CACHE_KEY_FIELD = "ID", CACHE_VAL_FIELD = "PAY_DATETIME";

  /** {@inheritDoc} */
  @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
    IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

    return cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalMode(WALMode.NONE)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(10 * 1024 * 1024)
                    .setPersistenceEnabled(true)));
  }

  /** {@inheritDoc} */
  @Override protected void beforeTest() throws Exception {
    super.beforeTest();
    cleanPersistenceDir();
  }

  /** {@inheritDoc} */
  @Override protected void afterTest() throws Exception {
    stopAllGrids();
    cleanPersistenceDir();
    super.afterTest();
  }

  /**
   *  Test destroy cache.
   *
   *  @throws Exception If failed.
   */
  @Test
  public void testDestroyCache() throws Exception {
    IgniteEx ignite = startGrid(0);
    ignite.cluster().active(true);
    QueryEntity entityV1 = createEntityV1(CACHE_NAME_1);
    IgniteCache<Long, BinaryObject> cache = startCacheByEntity(ignite, CACHE_NAME_1, entityV1);
    populateDataV1(ignite, cache);

    IgniteCacheObjectProcessor processor = ignite.context().cacheObjects();

    int typeId = processor.typeId(CACHE_VAL_TYPE);

    assertNotNull(processor.metadata(typeId));
    assertTrue(binaryFileExists(ignite.context(), typeId));

    cache.destroy();

    assertNull(processor.metadata(typeId));
    assertFalse(binaryFileExists(ignite.context(), typeId));

    QueryEntity entityV2 = createEntityV2(CACHE_NAME_1);
    cache = startCacheByEntity(ignite, CACHE_NAME_1, entityV2);
    populateDataV2(ignite, cache);

    assertNotNull(processor.metadata(typeId));
    assertTrue(binaryFileExists(ignite.context(), typeId));
  }

  /**
   *  Test destroy cache.
   *
   *  @throws Exception If failed.
   */
  @Test
  public void testDestroyMultipleCache() throws Exception {
    IgniteEx ignite = startGrid(0);
    ignite.cluster().active(true);
    IgniteCache<Long, BinaryObject> cache1 = startCacheByEntity(ignite, CACHE_NAME_1, createEntityV1(CACHE_NAME_1));
    populateDataV1(ignite, cache1);

    IgniteCache<Long, BinaryObject> cache2 = startCacheByEntity(ignite, CACHE_NAME_2, createEntityV1(CACHE_NAME_2));
    populateDataV1(ignite, cache2);

    IgniteCacheObjectProcessor processor = ignite.context().cacheObjects();

    int typeId = processor.typeId(CACHE_VAL_TYPE);

    assertNotNull(processor.metadata(typeId));
    assertTrue(binaryFileExists(ignite.context(), typeId));

    cache1.destroy();

    assertNotNull(processor.metadata(typeId));
    assertTrue(binaryFileExists(ignite.context(), typeId));

    cache2.destroy();

    assertNull(processor.metadata(typeId));
    assertFalse(binaryFileExists(ignite.context(), typeId));

    QueryEntity entityV2 = createEntityV2(CACHE_NAME_1);
    cache1 = startCacheByEntity(ignite, CACHE_NAME_1, entityV2);
    populateDataV2(ignite, cache1);

    assertNotNull(processor.metadata(typeId));
    assertTrue(binaryFileExists(ignite.context(), typeId));
  }

  @Test
  public void testMetadataAvailibilityAfterRestart() throws Exception {
    IgniteEx ignite = startGrid(1);
    ignite.cluster().active(true);
    QueryEntity entityV1 = createEntityV1(CACHE_NAME_1);
    IgniteCache<Long, BinaryObject> cache = startCacheByEntity(ignite, CACHE_NAME_1, entityV1);
    populateDataV1(ignite, cache);

    IgniteCacheObjectProcessor processor = ignite.context().cacheObjects();

    int typeId = processor.typeId(CACHE_VAL_TYPE);

    assertNotNull(processor.metadata(typeId));

    stopGrid(1);

    assertTrue(binaryFileExists(ignite.context(), typeId));

    ignite = startGrid(1);
    processor = ignite.context().cacheObjects();

    assertNotNull(processor.metadata(typeId));
  }

  private boolean binaryFileExists(GridKernalContext ctx, int typeId) throws IgniteCheckedException {
    final File binaryMateWokrDir = U.resolveBinaryMetaWorkDirectory(ctx);
    final File metaTypeFile = new File(binaryMateWokrDir, typeId + ".bin");
    return metaTypeFile.exists();
  }

  /**
   * @param ignite Ignite.
   */
  protected IgniteCache<Long, BinaryObject> startCacheByEntity(Ignite ignite, String cacheName, QueryEntity entity) {
    CacheConfiguration<Long, BinaryObject> ccfg = new CacheConfiguration<>(cacheName);
    ccfg.setQueryEntities(Collections.singletonList(entity));
    return ignite.createCache(ccfg).withKeepBinary();
  }

  private QueryEntity createEntityV1(String cacheName) {
    return createEntityByFieldType(cacheName, Date.class);
  }

  private QueryEntity createEntityV2(String cacheName) {
    return createEntityByFieldType(cacheName, Timestamp.class);
  }

  private QueryEntity createEntityByFieldType(String cacheName, Class<?> type) {
    Set<String> keys = Sets.newHashSet(CACHE_KEY_FIELD);
    QueryEntity entity = new QueryEntity(Long.class.getName(), CACHE_VAL_TYPE);
    entity.setTableName(cacheName).setKeyFields(keys)
            .addQueryField(CACHE_KEY_FIELD, Long.class.getName(), CACHE_KEY_FIELD);

    entity.addQueryField(CACHE_VAL_FIELD, type.getName(), CACHE_VAL_FIELD);
    return entity;
  }

  public void populateDataV1(Ignite ignite, IgniteCache<Long, BinaryObject> cache) {
    BinaryObject obj = buildBinaryObject(ignite, new Date(), Date.class);
    cache.put(1L, obj);
  }

  public void populateDataV2(Ignite ignite, IgniteCache<Long, BinaryObject> cache) {
    BinaryObject obj = buildBinaryObject(ignite, new Timestamp(U.currentTimeMillis()), Timestamp.class);
    cache.put(1L, obj);
  }

  private <T> BinaryObject buildBinaryObject(Ignite ignite, T value, Class<? super T> type) {
    BinaryObjectBuilder builder = ignite.binary().builder(CACHE_VAL_TYPE);
    builder.setField(CACHE_KEY_FIELD, 1L, Long.class);
    builder.setField(CACHE_VAL_FIELD, value, type);
    return builder.build();
  }
}
