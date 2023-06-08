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

import java.util.*;

import io.github.xiaodizi.audit.AuditLogContext;
import io.github.xiaodizi.audit.AuditLogEntryType;
import io.github.xiaodizi.auth.*;
import io.github.xiaodizi.config.DatabaseDescriptor;
import io.github.xiaodizi.schema.SchemaConstants;
import io.github.xiaodizi.cql3.*;
import io.github.xiaodizi.db.marshal.UTF8Type;
import io.github.xiaodizi.exceptions.InvalidRequestException;
import io.github.xiaodizi.exceptions.RequestExecutionException;
import io.github.xiaodizi.exceptions.RequestValidationException;
import io.github.xiaodizi.service.ClientState;
import io.github.xiaodizi.transport.messages.ResultMessage;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ListPermissionsStatement extends AuthorizationStatement
{
    private static final String KS = SchemaConstants.AUTH_KEYSPACE_NAME;
    private static final String CF = "permissions"; // virtual cf to use for now.

    private static final List<ColumnSpecification> metadata;

    static
    {
        List<ColumnSpecification> columns = new ArrayList<ColumnSpecification>(4);
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("role", true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("username", true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("resource", true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("permission", true), UTF8Type.instance));
        metadata = Collections.unmodifiableList(columns);
    }

    protected final Set<Permission> permissions;
    protected IResource resource;
    protected final boolean recursive;
    private final RoleResource grantee;

    public ListPermissionsStatement(Set<Permission> permissions, IResource resource, RoleName grantee, boolean recursive)
    {
        this.permissions = permissions;
        this.resource = resource;
        this.recursive = recursive;
        this.grantee = grantee.hasName()? RoleResource.role(grantee.getName()) : null;
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // a check to ensure the existence of the user isn't being leaked by user existence check.
        state.ensureNotAnonymous();

        if (resource != null)
        {
            resource = maybeCorrectResource(resource, state);
            if (!resource.exists())
                throw new InvalidRequestException(String.format("%s doesn't exist", resource));
        }

        if ((grantee != null) && !DatabaseDescriptor.getRoleManager().isExistingRole(grantee))
            throw new InvalidRequestException(String.format("%s doesn't exist", grantee));
   }

    public void authorize(ClientState state)
    {
        // checked in validate
    }

    // TODO: Create a new ResultMessage type (?). Rows will do for now.
    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        List<PermissionDetails> details = new ArrayList<PermissionDetails>();

        if (resource != null && recursive)
        {
            for (IResource r : Resources.chain(resource))
                details.addAll(list(state, r));
        }
        else
        {
            details.addAll(list(state, resource));
        }

        Collections.sort(details);
        return resultMessage(details);
    }

    private Set<PermissionDetails> list(ClientState state, IResource resource)
    throws RequestValidationException, RequestExecutionException
    {
        try
        {
            return DatabaseDescriptor.getAuthorizer().list(state.getUser(), permissions, resource, grantee);
        }
        catch (UnsupportedOperationException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    private ResultMessage resultMessage(List<PermissionDetails> details)
    {
        if (details.isEmpty())
            return new ResultMessage.Void();

        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(metadata);
        ResultSet result = new ResultSet(resultMetadata);
        for (PermissionDetails pd : details)
        {
            result.addColumnValue(UTF8Type.instance.decompose(pd.grantee));
            result.addColumnValue(UTF8Type.instance.decompose(pd.grantee));
            result.addColumnValue(UTF8Type.instance.decompose(pd.resource.toString()));
            result.addColumnValue(UTF8Type.instance.decompose(pd.permission.toString()));
        }
        return new ResultMessage.Rows(result);
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.LIST_PERMISSIONS);
    }
}
