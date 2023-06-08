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

import io.github.xiaodizi.cql3.CQLStatement;
import io.github.xiaodizi.cql3.QueryOptions;
import io.github.xiaodizi.cql3.QueryProcessor;
import io.github.xiaodizi.db.ConsistencyLevel;
import io.github.xiaodizi.distributed.api.IIsolatedExecutor;
import io.github.xiaodizi.service.ClientState;
import io.github.xiaodizi.service.ClientWarn;
import io.github.xiaodizi.service.QueryState;
import io.github.xiaodizi.transport.ProtocolVersion;
import io.github.xiaodizi.transport.messages.ResultMessage;
import io.github.xiaodizi.utils.ByteBufferUtil;
import io.github.xiaodizi.utils.FBUtilities;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.github.xiaodizi.utils.Clock.Global.nanoTime;

public class Query implements IIsolatedExecutor.SerializableCallable<Object[][]>
{
    private static final long serialVersionUID = 1L;

    final String query;
    final long timestamp;
    final org.apache.cassandra.distributed.api.ConsistencyLevel commitConsistencyOrigin;
    final org.apache.cassandra.distributed.api.ConsistencyLevel serialConsistencyOrigin;
    final Object[] boundValues;

    public Query(String query, long timestamp, org.apache.cassandra.distributed.api.ConsistencyLevel commitConsistencyOrigin, org.apache.cassandra.distributed.api.ConsistencyLevel serialConsistencyOrigin, Object[] boundValues)
    {
        this.query = query;
        this.timestamp = timestamp;
        this.commitConsistencyOrigin = commitConsistencyOrigin;
        this.serialConsistencyOrigin = serialConsistencyOrigin;
        this.boundValues = boundValues;
    }

    public Object[][] call()
    {
        ConsistencyLevel commitConsistency = toCassandraCL(commitConsistencyOrigin);
        ConsistencyLevel serialConsistency = serialConsistencyOrigin == null ? null : toCassandraCL(serialConsistencyOrigin);
        ClientState clientState = Coordinator.makeFakeClientState();
        CQLStatement prepared = QueryProcessor.getStatement(query, clientState);
        List<ByteBuffer> boundBBValues = new ArrayList<>();
        for (Object boundValue : boundValues)
            boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));

        prepared.validate(QueryState.forInternalCalls().getClientState());

        // Start capturing warnings on this thread. Note that this will implicitly clear out any previous
        // warnings as it sets a new State instance on the ThreadLocal.
        ClientWarn.instance.captureWarnings();

        ResultMessage res = prepared.execute(QueryState.forInternalCalls(),
                                             QueryOptions.create(commitConsistency,
                                                                 boundBBValues,
                                                                 false,
                                                                 Integer.MAX_VALUE,
                                                                 null,
                                                                 serialConsistency,
                                                                 ProtocolVersion.V4,
                                                                 null,
                                                                 timestamp,
                                                                 FBUtilities.nowInSeconds()),
                                             nanoTime());

        // Collect warnings reported during the query.
        if (res != null)
            res.setWarnings(ClientWarn.instance.getWarnings());

        return RowUtil.toQueryResult(res).toObjectArrays();
    }

    public String toString()
    {
        return String.format(query.replaceAll("\\?", "%s") + " AT " + commitConsistencyOrigin, boundValues);
    }

    static org.apache.cassandra.db.ConsistencyLevel toCassandraCL(org.apache.cassandra.distributed.api.ConsistencyLevel cl)
    {
        return org.apache.cassandra.db.ConsistencyLevel.fromCode(cl.ordinal());
    }

    static final org.apache.cassandra.distributed.api.ConsistencyLevel[] API_CLs = org.apache.cassandra.distributed.api.ConsistencyLevel.values();
    static org.apache.cassandra.distributed.api.ConsistencyLevel fromCassandraCL(org.apache.cassandra.db.ConsistencyLevel cl)
    {
        return API_CLs[cl.ordinal()];
    }

}