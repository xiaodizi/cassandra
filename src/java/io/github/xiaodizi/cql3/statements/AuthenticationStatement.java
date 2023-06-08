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

import io.github.xiaodizi.auth.Permission;
import io.github.xiaodizi.auth.RoleResource;
import io.github.xiaodizi.cql3.CQLStatement;
import io.github.xiaodizi.cql3.QueryOptions;
import io.github.xiaodizi.exceptions.RequestExecutionException;
import io.github.xiaodizi.exceptions.RequestValidationException;
import io.github.xiaodizi.exceptions.UnauthorizedException;
import io.github.xiaodizi.service.ClientState;
import io.github.xiaodizi.service.QueryState;
import io.github.xiaodizi.transport.messages.ResultMessage;

public abstract class AuthenticationStatement extends CQLStatement.Raw implements CQLStatement
{
    public AuthenticationStatement prepare(ClientState state)
    {
        return this;
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        return execute(state.getClientState());
    }

    public abstract ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException;

    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        // executeLocally is for local query only, thus altering users doesn't make sense and is not supported
        throw new UnsupportedOperationException();
    }

    public void checkPermission(ClientState state, Permission required, RoleResource resource) throws UnauthorizedException
    {
        try
        {
            state.ensurePermission(required, resource);
        }
        catch (UnauthorizedException e)
        {
            // Catch and rethrow with a more friendly message
            throw new UnauthorizedException(String.format("User %s does not have sufficient privileges " +
                                                          "to perform the requested operation",
                                                          state.getUser().getName()));
        }
    }

    public String obfuscatePassword(String query)
    {
        return query;
    }
}

