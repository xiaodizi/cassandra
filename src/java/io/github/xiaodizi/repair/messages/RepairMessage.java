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
package io.github.xiaodizi.repair.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xiaodizi.exceptions.RepairException;
import io.github.xiaodizi.exceptions.RequestFailureReason;
import io.github.xiaodizi.gms.Gossiper;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.net.Message;
import io.github.xiaodizi.net.MessagingService;
import io.github.xiaodizi.net.RequestCallback;
import io.github.xiaodizi.net.Verb;
import io.github.xiaodizi.repair.RepairJobDesc;
import io.github.xiaodizi.streaming.PreviewKind;
import io.github.xiaodizi.utils.CassandraVersion;
import io.github.xiaodizi.utils.TimeUUID;

import static io.github.xiaodizi.net.MessageFlag.CALL_BACK_ON_FAILURE;

/**
 * Base class of all repair related request/response messages.
 *
 * @since 2.0
 */
public abstract class RepairMessage
{
    private static final CassandraVersion SUPPORTS_TIMEOUTS = new CassandraVersion("4.0.7-SNAPSHOT");
    private static final Logger logger = LoggerFactory.getLogger(RepairMessage.class);
    public final RepairJobDesc desc;

    protected RepairMessage(RepairJobDesc desc)
    {
        this.desc = desc;
    }

    public interface RepairFailureCallback
    {
        void onFailure(Exception e);
    }

    public static void sendMessageWithFailureCB(RepairMessage request, Verb verb, InetAddressAndPort endpoint, RepairFailureCallback failureCallback)
    {
        RequestCallback<?> callback = new RequestCallback<Object>()
        {
            @Override
            public void onResponse(Message<Object> msg)
            {
                logger.info("[#{}] {} received by {}", request.desc.parentSessionId, verb, endpoint);
                // todo: at some point we should make repair messages follow the normal path, actually using this
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }

            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                logger.error("[#{}] {} failed on {}: {}", request.desc.parentSessionId, verb, from, failureReason);

                if (supportsTimeouts(from, request.desc.parentSessionId))
                    failureCallback.onFailure(RepairException.error(request.desc, PreviewKind.NONE, String.format("Got %s failure from %s: %s", verb, from, failureReason)));
            }
        };

        MessagingService.instance().sendWithCallback(Message.outWithFlag(verb, request, CALL_BACK_ON_FAILURE),
                                                     endpoint,
                                                     callback);
    }

    private static boolean supportsTimeouts(InetAddressAndPort from, TimeUUID parentSessionId)
    {
        CassandraVersion remoteVersion = Gossiper.instance.getReleaseVersion(from);
        if (remoteVersion != null && remoteVersion.compareTo(SUPPORTS_TIMEOUTS) >= 0)
            return true;
        logger.warn("[#{}] Not failing repair due to remote host {} not supporting repair message timeouts (version = {})", parentSessionId, from, remoteVersion);
        return false;
    }
}
