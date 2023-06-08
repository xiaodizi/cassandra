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
package io.github.xiaodizi.auth;

import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.cql3.functions.UDAggregate;
import io.github.xiaodizi.cql3.functions.UDFunction;
import io.github.xiaodizi.schema.KeyspaceMetadata;
import io.github.xiaodizi.schema.SchemaChangeListener;
import io.github.xiaodizi.schema.TableMetadata;

/**
 * SchemaChangeListener implementation that cleans up permissions on dropped resources.
 */
public class AuthSchemaChangeListener implements SchemaChangeListener
{
    @Override
    public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData)
    {
        DatabaseDescriptor.getAuthorizer().revokeAllOn(DataResource.keyspace(keyspace.name));
        DatabaseDescriptor.getAuthorizer().revokeAllOn(DataResource.allTables(keyspace.name));
        DatabaseDescriptor.getAuthorizer().revokeAllOn(FunctionResource.keyspace(keyspace.name));
    }

    @Override
    public void onDropTable(TableMetadata table, boolean dropData)
    {
        DatabaseDescriptor.getAuthorizer().revokeAllOn(DataResource.table(table.keyspace, table.name));
    }

    @Override
    public void onDropFunction(UDFunction function)
    {
        DatabaseDescriptor.getAuthorizer()
                          .revokeAllOn(FunctionResource.function(function.name().keyspace, function.name().name, function.argTypes()));
    }

    @Override
    public void onDropAggregate(UDAggregate aggregate)
    {
        DatabaseDescriptor.getAuthorizer()
                          .revokeAllOn(FunctionResource.function(aggregate.name().keyspace, aggregate.name().name, aggregate.argTypes()));
    }
}
