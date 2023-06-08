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

package io.github.xiaodizi.distributed.test.ring;

import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.distributed.Cluster;
import io.github.xiaodizi.distributed.api.IInstanceConfig;
import io.github.xiaodizi.distributed.api.IInvokableInstance;
import io.github.xiaodizi.distributed.api.TokenSupplier;
import io.github.xiaodizi.distributed.impl.DistributedTestSnitch;
import io.github.xiaodizi.distributed.shared.NetworkTopology;
import io.github.xiaodizi.distributed.test.TestBaseImpl;
import io.github.xiaodizi.locator.EndpointsForRange;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.service.PendingRangeCalculatorService;
import io.github.xiaodizi.service.StorageService;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.github.xiaodizi.distributed.action.GossipHelper.*;
import static io.github.xiaodizi.distributed.api.Feature.GOSSIP;
import static io.github.xiaodizi.distributed.api.Feature.NETWORK;

public class PendingWritesTest extends TestBaseImpl
{
    @Test
    public void testPendingWrites() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            BootstrapTest.populate(cluster, 0, 100);
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty("cassandra.join_ring", false,
                         () -> newInstance.startup(cluster));

            cluster.forEach(statusToBootstrap(newInstance));
            cluster.run(bootstrap(false, Duration.ofSeconds(60), Duration.ofSeconds(60)), newInstance.config().num());

            cluster.get(1).acceptsOnInstance((InetSocketAddress ip) -> {
                Set<InetAddressAndPort> set = new HashSet<>();
                for (Map.Entry<Range<Token>, EndpointsForRange.Builder> e : StorageService.instance.getTokenMetadata().getPendingRanges(KEYSPACE))
                {
                    set.addAll(e.getValue().build().endpoints());
                }
                Assert.assertEquals(set.size(), 1);
                Assert.assertTrue(String.format("%s should contain %s", set, ip),
                                  set.contains(DistributedTestSnitch.toCassandraInetAddressAndPort(ip)));
            }).accept(cluster.get(3).broadcastAddress());

            BootstrapTest.populate(cluster, 100, 150);

            newInstance.nodetoolResult("join").asserts().success();

            cluster.run(disseminateGossipState(newInstance),1, 2);

            cluster.run((instance) -> {
                instance.runOnInstance(() -> {
                    PendingRangeCalculatorService.instance.update();
                    PendingRangeCalculatorService.instance.blockUntilFinished();
                });
            }, 1, 2);

            cluster.get(1).acceptsOnInstance((InetSocketAddress ip) -> {
                Set<InetAddressAndPort> set = new HashSet<>();
                for (Map.Entry<Range<Token>, EndpointsForRange.Builder> e : StorageService.instance.getTokenMetadata().getPendingRanges(KEYSPACE))
                    set.addAll(e.getValue().build().endpoints());
                assert set.size() == 0 : set;
            }).accept(cluster.get(3).broadcastAddress());

            for (Map.Entry<Integer, Long> e : BootstrapTest.count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state", e.getValue().longValue(), 150L);
        }
    }
}
