package org.apache.ignite.cache.affinity;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class AffinityClientAssignmentTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODE_CNT = 2;

    /** */
    private static final String CACHE1 = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setBackups(0);
        ccfg.setName(CACHE1);
        ccfg.setStatisticsEnabled(true);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));

        if (igniteInstanceName.equals(getTestIgniteInstanceName(NODE_CNT - 1))) {
            cfg.setClientMode(true);
        }
        else
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODE_CNT - 1);

        startGrid(NODE_CNT - 1); // Start client after servers.
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNodeNotInAffinity() throws Exception {
        checkCache(CACHE1);
    }

    /**
     * @param cacheName Cache name.
     */
    private void checkCache(String cacheName) {
        log.info("Test cache: " + cacheName);

        Affinity<Object> aff0 = ignite(0).affinity(cacheName);

        Ignite client = ignite(NODE_CNT - 1);

        assertTrue(client.configuration().isClientMode());

        Affinity<Object> aff = client.affinity(cacheName);

        for (int part = 0; part < aff.partitions(); part++) {
            ClusterNode node = aff.mapPartitionToNode(part);

            assertEquals(aff0.mapPartitionToNode(part), node);

            assertFalse(node.isClient());

            assertEquals(aff0.partition(part), aff.partition(part));
        }
    }
}
