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
import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.cql3.RoleName;
import io.github.xiaodizi.exceptions.InvalidRequestException;
import io.github.xiaodizi.exceptions.RequestValidationException;
import io.github.xiaodizi.exceptions.UnauthorizedException;
import io.github.xiaodizi.service.ClientState;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public abstract class RoleManagementStatement extends AuthenticationStatement
{
    protected final RoleResource role;
    protected final RoleResource grantee;

    public RoleManagementStatement(RoleName name, RoleName grantee)
    {
        this.role = RoleResource.role(name.getName());
        this.grantee = RoleResource.role(grantee.getName());
    }

    public void authorize(ClientState state) throws UnauthorizedException
    {
        super.checkPermission(state, Permission.AUTHORIZE, role);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        state.ensureNotAnonymous();

        if (!DatabaseDescriptor.getRoleManager().isExistingRole(role))
            throw new InvalidRequestException(String.format("%s doesn't exist", role.getRoleName()));

        if (!DatabaseDescriptor.getRoleManager().isExistingRole(grantee))
            throw new InvalidRequestException(String.format("%s doesn't exist", grantee.getRoleName()));
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
