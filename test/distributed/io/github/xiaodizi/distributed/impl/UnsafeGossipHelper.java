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

package io.github.xiaodizi.distributed.impl;

import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.db.SystemKeyspace;
import io.github.xiaodizi.dht.IPartitioner;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.distributed.api.IInstance;
import io.github.xiaodizi.distributed.api.IInvokableInstance;
import io.github.xiaodizi.distributed.api.IIsolatedExecutor;
import io.github.xiaodizi.gms.ApplicationState;
import io.github.xiaodizi.gms.EndpointState;
import io.github.xiaodizi.gms.Gossiper;
import io.github.xiaodizi.gms.VersionedValue;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.net.MessagingService;
import io.github.xiaodizi.service.PendingRangeCalculatorService;
import io.github.xiaodizi.service.StorageService;
import io.github.xiaodizi.utils.FBUtilities;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.github.xiaodizi.distributed.impl.DistributedTestSnitch.toCassandraInetAddressAndPort;
import static io.github.xiaodizi.locator.InetAddressAndPort.getByAddress;
import static java.util.Collections.singleton;

public class UnsafeGossipHelper
{
    public static class HostInfo implements Serializable
    {
        final InetSocketAddress address;
        final UUID hostId;
        final String tokenString;
        final int messagingVersion;
        final boolean isShutdown;

        private HostInfo(InetSocketAddress address, UUID hostId, String tokenString, int messagingVersion, boolean isShutdown)
        {
            this.address = address;
            this.hostId = hostId;
            this.tokenString = tokenString;
            this.messagingVersion = messagingVersion;
            this.isShutdown = isShutdown;
        }

        private HostInfo(IInstance instance)
        {
            this(instance, instance.config().hostId(), instance.config().getString("initial_token"));
        }

        private HostInfo(IInstance instance, UUID hostId, String tokenString)
        {
            this(instance.broadcastAddress(), hostId, tokenString, instance.getMessagingVersion(), instance.isShutdown());
        }
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingRunner(IIsolatedExecutor.SerializableBiFunction<VersionedValue.VersionedValueFactory, Collection<Token>, VersionedValue> statusFactory, InetSocketAddress address, UUID hostId, String tokenString, int messagingVersion, boolean isShutdown)
    {
        return () -> {
            try
            {
                IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
                InetAddressAndPort addressAndPort = getByAddress(address);
                Token token;
                if (FBUtilities.getBroadcastAddressAndPort().equals(addressAndPort))
                {
                    // try grabbing saved tokens so that - if we're leaving - we get the ones we may have adopted as part of a range movement
                    // if that fails, grab them from config (as we're probably joining and should just use the default token)
                    Token.TokenFactory tokenFactory = DatabaseDescriptor.getPartitioner().getTokenFactory();
                    if (tokenString == null)
                    {
                        Token tmp;
                        try
                        {
                             tmp = getOnlyElement(SystemKeyspace.getSavedTokens());
                        }
                        catch (Throwable t)
                        {
                            tmp = tokenFactory.fromString(getOnlyElement(DatabaseDescriptor.getInitialTokens()));
                        }
                        token = tmp;
                    }
                    else
                    {
                        token = tokenFactory.fromString(tokenString);
                    }

                    SystemKeyspace.setLocalHostId(hostId);
                    SystemKeyspace.updateTokens(singleton(token));
                }
                else
                {
                    if (tokenString == null)
                        throw new IllegalArgumentException();

                    token = DatabaseDescriptor.getPartitioner().getTokenFactory().fromString(tokenString);
                }

                Gossiper.runInGossipStageBlocking(() -> {
                    EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(addressAndPort);
                    if (state == null)
                    {
                        Gossiper.instance.initializeNodeUnsafe(addressAndPort, hostId, 1);
                        state = Gossiper.instance.getEndpointStateForEndpoint(addressAndPort);
                        Gossiper.instance.realMarkAlive(addressAndPort, state);
                    }

                    state.addApplicationState(ApplicationState.TOKENS, new VersionedValue.VersionedValueFactory(partitioner).tokens(singleton(token)));
                    VersionedValue status = statusFactory.apply(new VersionedValue.VersionedValueFactory(partitioner), singleton(token));
                    state.addApplicationState(ApplicationState.STATUS_WITH_PORT, status);
                    StorageService.instance.onChange(addressAndPort, ApplicationState.STATUS_WITH_PORT, status);
                });

                int setMessagingVersion = isShutdown
                                          ? MessagingService.current_version
                                          : Math.min(MessagingService.current_version, messagingVersion);
                MessagingService.instance().versions.set(addressAndPort, setMessagingVersion);

                PendingRangeCalculatorService.instance.blockUntilFinished();
            }
            catch (Throwable e) // UnknownHostException
            {
                throw new RuntimeException(e);
            }
        };
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingNormalRunner(IInstance peer)
    {
        return addToRingNormalRunner(new HostInfo(peer));
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingNormalRunner(IInstance peer, UUID hostId, String tokenString)
    {
        return addToRingNormalRunner(new HostInfo(peer, hostId, tokenString));
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingNormalRunner(HostInfo info)
    {
        return addToRingNormalRunner(info.address, info.hostId, info.tokenString, info.messagingVersion, info.isShutdown);
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingNormalRunner(InetSocketAddress address, UUID hostId, String tokenString, int messagingVersion, boolean isShutdown)
    {
        return addToRingRunner(VersionedValue.VersionedValueFactory::normal, address, hostId, tokenString, messagingVersion, isShutdown);
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingRunner(IIsolatedExecutor.SerializableBiFunction<VersionedValue.VersionedValueFactory, Collection<Token>, VersionedValue> statusFactory, HostInfo info)
    {
        return addToRingRunner(statusFactory, info.address, info.hostId, info.tokenString, info.messagingVersion, info.isShutdown);
    }

    // reset gossip state so we know of the node being alive only
    public static IIsolatedExecutor.SerializableRunnable removeFromRingRunner(IInstance instance)
    {
        return removeFromRingRunner(new HostInfo(instance));
    }

    // reset gossip state so we know of the node being alive only
    public static IIsolatedExecutor.SerializableRunnable removeFromRingRunner(HostInfo info)
    {
        return removeFromRingRunner(info.address, info.hostId, info.tokenString);
    }

    public static IIsolatedExecutor.SerializableRunnable removeFromRingRunner(InetSocketAddress address, UUID hostId, String tokenString)
    {
        return () -> {

            try
            {
                IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
                Token token = partitioner.getTokenFactory().fromString(tokenString);
                InetAddressAndPort addressAndPort = toCassandraInetAddressAndPort(address);

                Gossiper.runInGossipStageBlocking(() -> {
                    StorageService.instance.onChange(addressAndPort,
                                                     ApplicationState.STATUS,
                                                     new VersionedValue.VersionedValueFactory(partitioner).left(singleton(token), 0L));
                    Gossiper.instance.unsafeAnnulEndpoint(addressAndPort);
                    Gossiper.instance.initializeNodeUnsafe(addressAndPort, hostId, 1);
                    Gossiper.instance.realMarkAlive(addressAndPort, Gossiper.instance.getEndpointStateForEndpoint(addressAndPort));
                });
                PendingRangeCalculatorService.instance.blockUntilFinished();
            }
            catch (Throwable e) // UnknownHostException
            {
                throw new RuntimeException(e);
            }
        };
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingBootstrappingRunner(IInstance peer)
    {
        return addToRingRunner(VersionedValue.VersionedValueFactory::bootstrapping, new HostInfo(peer));
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingBootstrapReplacingRunner(IInstance peer, IInvokableInstance replacing, UUID hostId, String tokenString)
    {
        return addToRingBootstrapReplacingRunner(peer, replacing.broadcastAddress(), hostId, tokenString);
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingBootstrapReplacingRunner(IInstance peer, InetSocketAddress replacingAddress, UUID hostId, String tokenString)
    {
        return addToRingRunner((factory, ignore) -> factory.bootReplacingWithPort(getByAddress(replacingAddress)), new HostInfo(peer, hostId, tokenString));
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingNormalReplacedRunner(IInstance peer, IInstance replaced)
    {
        return addToRingNormalReplacedRunner(peer, replaced.broadcastAddress());
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingNormalReplacedRunner(IInstance peer, InetSocketAddress replacedAddress)
    {
        return addToRingRunner((factory, ignore) -> factory.bootReplacingWithPort(getByAddress(replacedAddress)), new HostInfo(peer, null, null));
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingLeavingRunner(IInstance peer)
    {
        return addToRingRunner(VersionedValue.VersionedValueFactory::leaving, new HostInfo(peer, null, null));
    }

    public static IIsolatedExecutor.SerializableRunnable addToRingLeftRunner(IInstance peer)
    {
        return addToRingRunner((factory, tokens) -> factory.left(tokens, Long.MAX_VALUE), new HostInfo(peer, null, null));
    }

    public static void removeFromRing(IInstance peer)
    {
        removeFromRingRunner(peer).run();
    }

    public static void addToRingNormal(IInstance peer)
    {
        addToRingNormalRunner(peer).run();
        assert StorageService.instance.getTokenMetadata().isMember(toCassandraInetAddressAndPort(peer.broadcastAddress()));
    }

    public static void addToRingBootstrapping(IInstance peer)
    {
        addToRingBootstrappingRunner(peer).run();
    }

    public static IIsolatedExecutor.SerializableRunnable markShutdownRunner(InetSocketAddress address)
    {
        return () -> {
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            Gossiper.runInGossipStageBlocking(() -> {
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(getByAddress(address));
                VersionedValue status = new VersionedValue.VersionedValueFactory(partitioner).shutdown(true);
                state.addApplicationState(ApplicationState.STATUS, status);
                state.getHeartBeatState().forceHighestPossibleVersionUnsafe();
                StorageService.instance.onChange(getByAddress(address), ApplicationState.STATUS, status);
            });
        };
    }
}
