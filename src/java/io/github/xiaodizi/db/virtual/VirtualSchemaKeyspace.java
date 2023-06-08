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
package io.github.xiaodizi.db.virtual;

import com.google.common.collect.ImmutableList;

import io.github.xiaodizi.db.marshal.BytesType;
import io.github.xiaodizi.db.marshal.Int32Type;
import io.github.xiaodizi.db.marshal.UTF8Type;
import io.github.xiaodizi.dht.LocalPartitioner;
import io.github.xiaodizi.schema.ColumnMetadata;
import io.github.xiaodizi.schema.KeyspaceMetadata;
import io.github.xiaodizi.schema.TableMetadata;

import static io.github.xiaodizi.schema.SchemaConstants.VIRTUAL_SCHEMA;
import static io.github.xiaodizi.schema.TableMetadata.builder;

public final class VirtualSchemaKeyspace extends VirtualKeyspace
{
    public static final VirtualSchemaKeyspace instance = new VirtualSchemaKeyspace();

    private VirtualSchemaKeyspace()
    {
        super(VIRTUAL_SCHEMA, ImmutableList.of(new VirtualKeyspaces(VIRTUAL_SCHEMA), new VirtualTables(VIRTUAL_SCHEMA), new VirtualColumns(VIRTUAL_SCHEMA)));
    }

    private static final class VirtualKeyspaces extends AbstractVirtualTable
    {
        private static final String KEYSPACE_NAME = "keyspace_name";

        private VirtualKeyspaces(String keyspace)
        {
            super(builder(keyspace, "keyspaces")
                 .comment("virtual keyspace definitions")
                 .kind(TableMetadata.Kind.VIRTUAL)
                 .partitioner(new LocalPartitioner(UTF8Type.instance))
                 .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                 .build());
        }

        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            for (KeyspaceMetadata keyspace : VirtualKeyspaceRegistry.instance.virtualKeyspacesMetadata())
                result.row(keyspace.name);
            return result;
        }
    }

    private static final class VirtualTables extends AbstractVirtualTable
    {
        private static final String KEYSPACE_NAME = "keyspace_name";
        private static final String TABLE_NAME = "table_name";
        private static final String COMMENT = "comment";

        private VirtualTables(String keyspace)
        {
            super(builder(keyspace, "tables")
                 .comment("virtual table definitions")
                 .kind(TableMetadata.Kind.VIRTUAL)
                 .partitioner(new LocalPartitioner(UTF8Type.instance))
                 .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                 .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                 .addRegularColumn(COMMENT, UTF8Type.instance)
                 .build());
        }

        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());

            for (KeyspaceMetadata keyspace : VirtualKeyspaceRegistry.instance.virtualKeyspacesMetadata())
            {
                for (TableMetadata table : keyspace.tables)
                {
                    result.row(table.keyspace, table.name)
                          .column(COMMENT, table.params.comment);
                }
            }

            return result;
        }
    }

    private static final class VirtualColumns extends AbstractVirtualTable
    {
        private static final String KEYSPACE_NAME = "keyspace_name";
        private static final String TABLE_NAME = "table_name";
        private static final String COLUMN_NAME = "column_name";
        private static final String CLUSTERING_ORDER = "clustering_order";
        private static final String COLUMN_NAME_BYTES = "column_name_bytes";
        private static final String KIND = "kind";
        private static final String POSITION = "position";
        private static final String TYPE = "type";

        private VirtualColumns(String keyspace)
        {
            super(builder(keyspace, "columns")
                 .comment("virtual column definitions")
                 .kind(TableMetadata.Kind.VIRTUAL)
                 .partitioner(new LocalPartitioner(UTF8Type.instance))
                 .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                 .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                 .addClusteringColumn(COLUMN_NAME, UTF8Type.instance)
                 .addRegularColumn(CLUSTERING_ORDER, UTF8Type.instance)
                 .addRegularColumn(COLUMN_NAME_BYTES, BytesType.instance)
                 .addRegularColumn(KIND, UTF8Type.instance)
                 .addRegularColumn(POSITION, Int32Type.instance)
                 .addRegularColumn(TYPE, UTF8Type.instance)
                 .build());
        }

        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());

            for (KeyspaceMetadata keyspace : VirtualKeyspaceRegistry.instance.virtualKeyspacesMetadata())
            {
                for (TableMetadata table : keyspace.tables)
                {
                    for (ColumnMetadata column : table.columns())
                    {
                        result.row(column.ksName, column.cfName, column.name.toString())
                              .column(CLUSTERING_ORDER, column.clusteringOrder().toString().toLowerCase())
                              .column(COLUMN_NAME_BYTES, column.name.bytes)
                              .column(KIND, column.kind.toString().toLowerCase())
                              .column(POSITION, column.position())
                              .column(TYPE, column.type.asCQL3Type().toString());
                    }
                }
            }

            return result;
        }
    }
}
