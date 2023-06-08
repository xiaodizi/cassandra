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
package io.github.xiaodizi.repair.state;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.github.xiaodizi.dht.Range;
import io.github.xiaodizi.dht.Token;
import io.github.xiaodizi.locator.InetAddressAndPort;
import io.github.xiaodizi.repair.messages.PrepareMessage;
import io.github.xiaodizi.schema.TableId;
import io.github.xiaodizi.streaming.PreviewKind;
import io.github.xiaodizi.utils.TimeUUID;

public class ParticipateState extends AbstractCompletable<TimeUUID>
{
    public final InetAddressAndPort initiator;
    public final List<TableId> tableIds;
    public final Collection<Range<Token>> ranges;
    public final boolean incremental;
    public final long repairedAt;
    public final boolean global;
    public final PreviewKind previewKind;

    private final ConcurrentMap<UUID, ValidationState> validations = new ConcurrentHashMap<>();

    public final Phase phase = new Phase();

    public ParticipateState(InetAddressAndPort initiator, PrepareMessage msg)
    {
        super(msg.parentRepairSession);
        this.initiator = initiator;
        this.tableIds = msg.tableIds;
        this.ranges = msg.ranges;
        this.incremental = msg.isIncremental;
        this.repairedAt = msg.repairedAt;
        this.global = msg.isGlobal;
        this.previewKind = msg.previewKind;
    }

    public boolean register(ValidationState state)
    {
        ValidationState current = validations.putIfAbsent(state.id, state);
        return current == null;
    }

    public Collection<ValidationState> validations()
    {
        return validations.values();
    }

    public Collection<UUID> validationIds()
    {
        return validations.keySet();
    }

    public class Phase extends BasePhase
    {

    }
}
