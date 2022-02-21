/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.binary.compress;

import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

/**
 * Compressor test suite.
 */
@RunWith(DynamicSuite.class)
public class CompressorTestSuite {
    /**
     * @return Suite.
     */
    public static List<Class<?>> suite() {
        List<Class<?>> suite = new ArrayList<>();
        suite.add(BinaryObjectCompressionPerformanceTest.class);
        return suite;
    }
}
