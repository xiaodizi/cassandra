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
package io.github.xiaodizi.cql3.statements;

import java.util.concurrent.TimeoutException;

import io.github.xiaodizi.audit.AuditLogContext;
import io.github.xiaodizi.audit.AuditLogEntryType;
import io.github.xiaodizi.auth.Permission;
import io.github.xiaodizi.cql3.*;
import io.github.xiaodizi.db.ColumnFamilyStore;
import io.github.xiaodizi.db.Keyspace;
import io.github.xiaodizi.db.guardrails.Guardrails;
import io.github.xiaodizi.db.virtual.VirtualKeyspaceRegistry;
import io.github.xiaodizi.exceptions.*;
import io.github.xiaodizi.schema.Schema;
import io.github.xiaodizi.schema.TableId;
import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.service.ClientState;
import io.github.xiaodizi.service.QueryState;
import io.github.xiaodizi.service.StorageProxy;
import io.github.xiaodizi.transport.messages.ResultMessage;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class TruncateStatement extends QualifiedStatement implements CQLStatement
{
    public TruncateStatement(QualifiedName name)
    {
        super(name);
    }

    public TruncateStatement prepare(ClientState state)
    {
        return this;
    }

    public void authorize(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.ensureTablePermission(keyspace(), name(), Permission.MODIFY);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        Schema.instance.validateTable(keyspace(), name());
        Guardrails.dropTruncateTableEnabled.ensureEnabled(state);
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException, TruncateException
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), name());
            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (metaData.isVirtual())
            {
                executeForVirtualTable(metaData.id);
            }
            else
            {
                StorageProxy.truncateBlocking(keyspace(), name());
            }
        }
        catch (UnavailableException | TimeoutException e)
        {
            throw new TruncateException(e);
        }
        return null;
    }

    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), name());
            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (metaData.isVirtual())
            {
                executeForVirtualTable(metaData.id);
            }
            else
            {
                ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(name());
                cfs.truncateBlocking();
            }
        }
        catch (Exception e)
        {
            throw new TruncateException(e);
        }
        return null;
    }

    private void executeForVirtualTable(TableId id)
    {
        VirtualKeyspaceRegistry.instance.getTableNullable(id).truncate();
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.TRUNCATE, keyspace(), name());
    }
}
