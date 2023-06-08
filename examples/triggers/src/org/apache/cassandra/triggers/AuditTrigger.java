/**
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
package io.github.xiaodizi.triggers;

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import io.github.xiaodizi.schema.TableMetadata;
import io.github.xiaodizi.schema.Schema;
import io.github.xiaodizi.db.Mutation;
import io.github.xiaodizi.db.partitions.Partition;
import io.github.xiaodizi.db.partitions.PartitionUpdate;
import io.github.xiaodizi.io.util.FileUtils;
import io.github.xiaodizi.utils.FBUtilities;
import io.github.xiaodizi.utils.TimeUUID;

public class AuditTrigger implements ITrigger
{
    private static final String AUDIT_PROPERTIES_FILE_NAME = "AuditTrigger.properties";

    private final Properties properties;
    private final String auditKeyspace;
    private final String auditTable;

    public AuditTrigger()
    {
        properties = loadProperties();
        auditKeyspace = properties.getProperty("keyspace");
        auditTable = properties.getProperty("table");
    }

    public Collection<Mutation> augment(Partition update)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(auditKeyspace, auditTable);
        PartitionUpdate.SimpleBuilder audit = PartitionUpdate.simpleBuilder(metadata, TimeUUID.Generator.nextTimeUUID());

        audit.row()
             .add("keyspace_name", update.metadata().keyspace)
             .add("table_name", update.metadata().name)
             .add("primary_key", update.metadata().partitionKeyType.getString(update.partitionKey().getKey()));

        return Collections.singletonList(audit.buildAsMutation());
    }

    private static Properties loadProperties()
    {
        Properties properties = new Properties();
        InputStream stream = AuditTrigger.class.getClassLoader().getResourceAsStream(AUDIT_PROPERTIES_FILE_NAME);
        try
        {
            properties.load(stream);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
        return properties;
    }
}
