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

import com.google.common.collect.Iterators;
import io.github.xiaodizi.cql3.CQLStatement;
import io.github.xiaodizi.cql3.QueryOptions;
import io.github.xiaodizi.cql3.QueryProcessor;
import io.github.xiaodizi.cql3.statements.SelectStatement;
import io.github.xiaodizi.distributed.api.*;
import io.github.xiaodizi.service.ClientState;
import io.github.xiaodizi.service.ClientWarn;
import io.github.xiaodizi.service.QueryState;
import io.github.xiaodizi.service.reads.thresholds.CoordinatorWarnings;
import io.github.xiaodizi.tracing.Tracing;
import io.github.xiaodizi.transport.ProtocolVersion;
import io.github.xiaodizi.transport.messages.ResultMessage;
import io.github.xiaodizi.utils.ByteBufferUtil;
import io.github.xiaodizi.utils.FBUtilities;
import io.github.xiaodizi.utils.TimeUUID;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import static io.github.xiaodizi.utils.Clock.Global.nanoTime;

public class Coordinator implements ICoordinator
{
    final Instance instance;
    public Coordinator(Instance instance)
    {
        this.instance = instance;
    }

    @Override
    public SimpleQueryResult executeWithResult(String query, ConsistencyLevel consistencyLevel, Object... boundValues)
    {
        return instance().sync(() -> unsafeExecuteInternal(query, consistencyLevel, boundValues)).call();
    }

    public Future<SimpleQueryResult> asyncExecuteWithTracingWithResult(UUID sessionId, String query, ConsistencyLevel consistencyLevelOrigin, Object... boundValues)
    {
        return instance.async(() -> {
            try
            {
                Tracing.instance.newSession(TimeUUID.fromUuid(sessionId), Collections.emptyMap());
                return unsafeExecuteInternal(query, consistencyLevelOrigin, boundValues);
            }
            finally
            {
                Tracing.instance.stopSession();
            }
        }).call();
    }

    public static org.apache.cassandra.db.ConsistencyLevel toCassandraCL(ConsistencyLevel cl)
    {
        try
        {
            return org.apache.cassandra.db.ConsistencyLevel.fromCode(cl.code);
        }
        catch (NoSuchFieldError e)
        {
            return org.apache.cassandra.db.ConsistencyLevel.fromCode(cl.ordinal());
        }
    }

    protected static org.apache.cassandra.db.ConsistencyLevel toCassandraSerialCL(ConsistencyLevel cl)
    {
        return toCassandraCL(cl == null ? ConsistencyLevel.SERIAL : cl);
    }

    public static SimpleQueryResult unsafeExecuteInternal(String query, ConsistencyLevel consistencyLevel, Object[] boundValues)
    {
        return unsafeExecuteInternal(query, null, consistencyLevel, boundValues);
    }

    public static SimpleQueryResult unsafeExecuteInternal(String query, ConsistencyLevel serialConsistencyLevel, ConsistencyLevel commitConsistencyLevel, Object[] boundValues)
    {
        ClientState clientState = makeFakeClientState();
        CQLStatement prepared = QueryProcessor.getStatement(query, clientState);
        List<ByteBuffer> boundBBValues = new ArrayList<>();
        for (Object boundValue : boundValues)
            boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));

        prepared.validate(QueryState.forInternalCalls().getClientState());

        // Start capturing warnings on this thread. Note that this will implicitly clear out any previous 
        // warnings as it sets a new State instance on the ThreadLocal.
        ClientWarn.instance.captureWarnings();
        CoordinatorWarnings.init();
        try
        {
            ResultMessage res = prepared.execute(QueryState.forInternalCalls(),
                                   QueryOptions.create(toCassandraCL(commitConsistencyLevel),
                                                       boundBBValues,
                                                       false,
                                                       Integer.MAX_VALUE,
                                                       null,
                                                       toCassandraSerialCL(serialConsistencyLevel),
                                                       ProtocolVersion.CURRENT,
                                                       null),
                                   nanoTime());
            // Collect warnings reported during the query.
            CoordinatorWarnings.done();
            if (res != null)
                res.setWarnings(ClientWarn.instance.getWarnings());

            return RowUtil.toQueryResult(res);
        }
        catch (Exception | Error e)
        {
            CoordinatorWarnings.done();
            throw e;
        }
        finally
        {
            CoordinatorWarnings.reset();
            ClientWarn.instance.resetWarnings();
        }
    }

    public Object[][] executeWithTracing(UUID sessionId, String query, ConsistencyLevel consistencyLevelOrigin, Object... boundValues)
    {
        return IsolatedExecutor.waitOn(asyncExecuteWithTracing(sessionId, query, consistencyLevelOrigin, boundValues));
    }

    public IInstance instance()
    {
        return instance;
    }

    @Override
    public SimpleQueryResult executeWithResult(String query, ConsistencyLevel serialConsistencyLevel, ConsistencyLevel commitConsistencyLevel, Object... boundValues)
    {
        return instance.sync(() -> unsafeExecuteInternal(query, serialConsistencyLevel, commitConsistencyLevel, boundValues)).call();
    }

    @Override
    public QueryResult executeWithPagingWithResult(String query, ConsistencyLevel consistencyLevelOrigin, int pageSize, Object... boundValues)
    {
        if (pageSize <= 0)
            throw new IllegalArgumentException("Page size should be strictly positive but was " + pageSize);

        return instance.sync(() -> {
            ClientState clientState = makeFakeClientState();
            ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelOrigin.name());
            CQLStatement prepared = QueryProcessor.getStatement(query, clientState);
            final List<ByteBuffer> boundBBValues = new ArrayList<>();
            for (Object boundValue : boundValues)
                boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));

            prepared.validate(clientState);
            assert prepared instanceof SelectStatement : "Only SELECT statements can be executed with paging";

            long nanoTime = nanoTime();
            SelectStatement selectStatement = (SelectStatement) prepared;

            QueryState queryState = new QueryState(clientState);
            QueryOptions initialOptions = QueryOptions.create(toCassandraCL(consistencyLevel),
                                                              boundBBValues,
                                                              false,
                                                              pageSize,
                                                              null,
                                                              null,
                                                              ProtocolVersion.CURRENT,
                                                              selectStatement.keyspace());


            ResultMessage.Rows initialRows = selectStatement.execute(queryState, initialOptions, nanoTime);
            Iterator<Object[]> iter = new Iterator<Object[]>() {
                ResultMessage.Rows rows = selectStatement.execute(queryState, initialOptions, nanoTime);
                Iterator<Object[]> iter = RowUtil.toIter(rows);

                public boolean hasNext()
                {
                    if (iter.hasNext())
                        return true;

                    if (rows.result.metadata.getPagingState() == null)
                        return false;

                    QueryOptions nextOptions = QueryOptions.create(toCassandraCL(consistencyLevel),
                                                                   boundBBValues,
                                                                   true,
                                                                   pageSize,
                                                                   rows.result.metadata.getPagingState(),
                                                                   null,
                                                                   ProtocolVersion.CURRENT,
                                                                   selectStatement.keyspace());

                    rows = selectStatement.execute(queryState, nextOptions, nanoTime);
                    iter = Iterators.forArray(RowUtil.toObjects(initialRows.result.metadata.names, rows.result.rows));

                    return hasNext();
                }

                public Object[] next()
                {
                    return iter.next();
                }
            };

            return QueryResults.fromObjectArrayIterator(RowUtil.getColumnNames(initialRows.result.metadata.names), iter);
        }).call();
    }

    public static ClientState makeFakeClientState()
    {
        return ClientState.forExternalCalls(new InetSocketAddress(FBUtilities.getJustLocalAddress(), 9042));
    }
}
