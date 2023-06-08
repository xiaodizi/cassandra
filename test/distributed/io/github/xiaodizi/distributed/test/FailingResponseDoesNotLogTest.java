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

import com.google.common.collect.ImmutableMap;
import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.cql3.BatchQueryOptions;
import io.github.xiaodizi.cql3.CQLStatement;
import io.github.xiaodizi.cql3.QueryHandler;
import io.github.xiaodizi.cql3.QueryOptions;
import io.github.xiaodizi.cql3.statements.BatchStatement;
import io.github.xiaodizi.db.ConsistencyLevel;
import io.github.xiaodizi.distributed.Cluster;
import io.github.xiaodizi.distributed.api.Feature;
import io.github.xiaodizi.distributed.api.LogAction;
import io.github.xiaodizi.distributed.api.LogResult;
import io.github.xiaodizi.exceptions.RequestExecutionException;
import io.github.xiaodizi.exceptions.RequestValidationException;
import io.github.xiaodizi.service.ClientState;
import io.github.xiaodizi.service.QueryState;
import io.github.xiaodizi.transport.SimpleClient;
import io.github.xiaodizi.transport.messages.ResultMessage;
import io.github.xiaodizi.utils.MD5Digest;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class is rather impelemntation specific.  It is possible that changes made will cause this tests to fail,
 * so updating to the latest logic is fine.
 *
 * This class makes sure we do not do logging/update metrics for client from a specific set of ip domains, so as long
 * as we still do not log/update metrics, then the test is doing the right thing.
 */
public class FailingResponseDoesNotLogTest extends TestBaseImpl
{
    @BeforeClass
    public static void beforeClassTopLevel() // need to make sure not to conflict with TestBaseImpl.beforeClass
    {

        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void dispatcherErrorDoesNotLock() throws IOException
    {
        System.setProperty("cassandra.custom_query_handler_class", AlwaysRejectErrorQueryHandler.class.getName());
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP)
                                                        .set("client_error_reporting_exclusions", ImmutableMap.of("subnets", Collections.singletonList("127.0.0.1")))
                                      )
                                      .start())
        {
            try (SimpleClient client = SimpleClient.builder("127.0.0.1", 9042).build().connect(false))
            {
                client.execute("SELECT * FROM system.peers", ConsistencyLevel.ONE);
                Assert.fail("Query should have failed");
            }
            catch (Exception e)
            {
                // ignore; expected
            }

            // logs happen before client response; so grep is enough
            LogAction logs = cluster.get(1).logs();
            LogResult<List<String>> matches = logs.grep("address contained in client_error_reporting_exclusions");
            Assertions.assertThat(matches.getResult()).hasSize(1);
            matches = logs.grep("Unexpected exception during request");
            Assertions.assertThat(matches.getResult()).isEmpty();
        }
        finally
        {
            System.clearProperty("cassandra.custom_query_handler_class");
        }
    }

    public static class AlwaysRejectErrorQueryHandler implements QueryHandler
    {
        @Override
        public CQLStatement parse(String queryString, QueryState queryState, QueryOptions options)
        {
            throw new AssertionError("reject");
        }

        @Override
        public ResultMessage process(CQLStatement statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
        {
            throw new AssertionError("reject");
        }

        @Override
        public ResultMessage.Prepared prepare(String query, ClientState clientState, Map<String, ByteBuffer> customPayload) throws RequestValidationException
        {
            throw new AssertionError("reject");
        }

        @Override
        public Prepared getPrepared(MD5Digest id)
        {
            throw new AssertionError("reject");
        }

        @Override
        public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
        {
            throw new AssertionError("reject");
        }

        @Override
        public ResultMessage processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
        {
            throw new AssertionError("reject");
        }
    }
}
