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

import io.github.xiaodizi.distributed.Cluster;
import io.github.xiaodizi.gms.EndpointState;
import io.github.xiaodizi.gms.IEndpointStateChangeSubscriber;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.service.StorageService;
import io.github.xiaodizi.utils.concurrent.Condition;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.xiaodizi.distributed.api.ConsistencyLevel.ALL;
import static io.github.xiaodizi.distributed.api.Feature.GOSSIP;
import static io.github.xiaodizi.distributed.api.Feature.NETWORK;
import static io.github.xiaodizi.gms.Gossiper.instance;
import static io.github.xiaodizi.net.Verb.GOSSIP_DIGEST_ACK;
import static io.github.xiaodizi.net.Verb.GOSSIP_DIGEST_SYN;
import static io.github.xiaodizi.utils.concurrent.Condition.newOneTimeCondition;
import static java.lang.Thread.sleep;

public class GossipShutdownTest extends TestBaseImpl
{
    /**
     * Makes sure that a node that has shutdown doesn't come back as live (without being restarted)
     */
    @Test
    public void shutdownStayDownTest() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(config -> config.with(GOSSIP)
                                                                         .with(NETWORK))
                                             .start()))
        {
            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, v int)");

            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, v) values (?,?)", ALL, i, i);

            Condition timeToShutdown = newOneTimeCondition();
            Condition waitForShutdown = newOneTimeCondition();
            AtomicBoolean signalled = new AtomicBoolean(false);
            Future f = es.submit(() -> {
                await(timeToShutdown);

                cluster.get(1).runOnInstance(() -> {
                    instance.register(new EPChanges());
                });

                cluster.get(2).runOnInstance(() -> {
                    StorageService.instance.setIsShutdownUnsafeForTests(true);
                    instance.stop();
                });
                waitForShutdown.signalAll();
            });

            cluster.filters().outbound().from(2).to(1).verbs(GOSSIP_DIGEST_SYN.id).messagesMatching((from, to, message) -> true).drop();
            cluster.filters().outbound().from(2).to(1).verbs(GOSSIP_DIGEST_ACK.id).messagesMatching((from, to, message) ->
                                                                                                         {
                                                                                                             if (signalled.compareAndSet(false, true))
                                                                                                             {
                                                                                                                 timeToShutdown.signalAll();
                                                                                                                 await(waitForShutdown);
                                                                                                                 return false;
                                                                                                             }
                                                                                                             return true;
                                                                                                         }).drop();

            sleep(10000); // wait for gossip to exchange a few messages
            f.get();
        }
        finally
        {
            es.shutdown();
        }
    }

    private static void await(Condition sc)
    {
        try
        {
            sc.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class EPChanges implements IEndpointStateChangeSubscriber, Serializable
    {
        private volatile boolean wasDead = false;
        public void onAlive(InetAddressAndPort endpoint, EndpointState state)
        {
            if (wasDead)
                throw new RuntimeException("Node should not go live after it has been dead.");
        }
        public void onDead(InetAddressAndPort endpoint, EndpointState state)
        {
            wasDead = true;
        }
    };
}
