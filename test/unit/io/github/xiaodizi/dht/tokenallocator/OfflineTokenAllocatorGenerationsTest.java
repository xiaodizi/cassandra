/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.xiaodizi.dht.tokenallocator;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xiaodizi.dht.IPartitioner;
import io.github.xiaodizi.dht.Murmur3Partitioner;
import io.github.xiaodizi.dht.RandomPartitioner;
import io.github.xiaodizi.tools.Util;

import static io.github.xiaodizi.dht.tokenallocator.OfflineTokenAllocator.allocate;
import static io.github.xiaodizi.dht.tokenallocator.OfflineTokenAllocatorTestUtils.assertTokensAndNodeCount;
import static io.github.xiaodizi.dht.tokenallocator.OfflineTokenAllocatorTestUtils.makeRackCountArray;


/**
 * We break the testTokenGenerations test out as it runs long and pushes the rest of the test suite to timeout on both
 * ci environments and local laptops.
 */
public class OfflineTokenAllocatorGenerationsTest
{
    private static final Logger logger = LoggerFactory.getLogger(OfflineTokenAllocatorGenerationsTest.class);

    @Before
    public void setup()
    {
        Util.initDatabaseDescriptor();
    }

    // We run with a subset of even, odd, boundary, etc. combinations, however we can't afford to walk through every entry
    // for each parameter we test as the tests end up taking too long and timing out.
    private final int[] racks = { 1, 2, 3, 5, 6, 9, 10 };
    private final int[] rfs = { 1, 2, 3, 5 };
    private final int[] tokens = { 1, 2, 3, 5, 6, 9, 10, 13, 15, 16 };

    /**
     * Cycle through a matrix of valid ranges.
     */
    @Test
    public void testTokenGenerations()
    {
        for (int numTokens : tokens)
        {
            for (int rf : rfs)
            {
                int nodeCount = 32;
                for (int rack: racks)
                {
                    int[] nodeToRack = makeRackCountArray(nodeCount, rack);
                    for (IPartitioner partitioner : new IPartitioner[] { Murmur3Partitioner.instance, RandomPartitioner.instance })
                    {
                        logger.info("Testing offline token allocator for numTokens={}, rf={}, racks={}, nodeToRack={}, partitioner={}",
                                    numTokens, rf, rack, nodeToRack, partitioner);
                        assertTokensAndNodeCount(numTokens, nodeCount, allocate(rf,
                                                                                numTokens,
                                                                                nodeToRack,
                                                                                new OfflineTokenAllocatorTestUtils.SystemOutputImpl(rf, rack),
                                                                                partitioner));
                    }
                }
            }
        }
    }
}
