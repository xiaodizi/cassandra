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

package io.github.xiaodizi.distributed.test;

import io.github.xiaodizi.cql3.QueryOptions;
import io.github.xiaodizi.distributed.api.ConsistencyLevel;
import io.github.xiaodizi.distributed.api.Feature;
import io.github.xiaodizi.distributed.api.ICluster;
import io.github.xiaodizi.distributed.api.IInvokableInstance;
import io.github.xiaodizi.fqltool.FQLQuery;
import io.github.xiaodizi.fqltool.QueryReplayer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueryReplayerEndToEndTest extends TestBaseImpl
{
    private final AtomicLong queryStartTimeGenerator = new AtomicLong(1000);
    private final AtomicInteger ckGenerator = new AtomicInteger(1);

    @Test
    public void testReplayAndCloseMultipleTimes() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(3)
                                                                  .withConfig(conf -> conf.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP, Feature.NETWORK))
                                                                  .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            List<String> hosts = cluster.stream()
                                        .map(i -> i.config().broadcastAddress().getAddress().getHostAddress())
                                        .collect(Collectors.toList());

            final int queriesCount = 3;
            // replay for the first time, it should pass
            replayAndClose(Collections.singletonList(makeFQLQueries(queriesCount)), hosts);
            // replay for the second time, it should pass too
            // however, if the cached sessions are not released, the second replay will reused the closed sessions from previous replay and fail to insert
            replayAndClose(Collections.singletonList(makeFQLQueries(queriesCount)), hosts);
            Object[][] result = cluster.coordinator(1)
                                       .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                ConsistencyLevel.QUORUM, 1);
            Assert.assertEquals(String.format("Expecting to see %d rows since it replayed twice and each with %d queries", queriesCount * 2, queriesCount),
                                queriesCount * 2, result.length);
        }
    }

    private void replayAndClose(List<List<FQLQuery>> allFqlQueries, List<String> hosts) throws IOException
    {
        List<Predicate<FQLQuery>> allowAll = Collections.singletonList(fqlQuery -> true);
        try (QueryReplayer queryReplayer = new QueryReplayer(allFqlQueries.iterator(), hosts, null, allowAll, null))
        {
            queryReplayer.replay();
        }
    }

    // generate a new list of FQLQuery for each invocation
    private List<FQLQuery> makeFQLQueries(int n)
    {
        return IntStream.range(0, n)
                        .boxed()
                        .map(i -> new FQLQuery.Single(KEYSPACE,
                                                      QueryOptions.DEFAULT.getProtocolVersion().asInt(),
                                                      QueryOptions.DEFAULT, queryStartTimeGenerator.incrementAndGet(),
                                                      2222,
                                                      3333,
                                                      String.format("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, %d, %d)", KEYSPACE, ckGenerator.incrementAndGet(), i),
                                                      Collections.emptyList()))
                        .collect(Collectors.toList());
    }
}
