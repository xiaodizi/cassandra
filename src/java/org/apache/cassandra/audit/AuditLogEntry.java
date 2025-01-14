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
package org.apache.cassandra.audit;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import javax.annotation.Nullable;

import com.alibaba.fastjson2.JSON;
import org.apache.cassandra.audit.es.*;
import org.apache.cassandra.audit.es.dto.Hites;
import org.apache.cassandra.audit.es.res.DataRsp;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class AuditLogEntry {

    private static final Logger logger = LoggerFactory.getLogger(AuditLogEntry.class);

    private final InetAddressAndPort host = FBUtilities.getBroadcastAddressAndPort();
    private final InetAddressAndPort source;
    private final String user;
    private final long timestamp;
    private final AuditLogEntryType type;
    private final UUID batch;
    private final String keyspace;
    private final String scope;
    private final String operation;
    private final QueryOptions options;
    private final QueryState state;

    private AuditLogEntry(AuditLogEntryType type,
                          InetAddressAndPort source,
                          String user,
                          long timestamp,
                          UUID batch,
                          String keyspace,
                          String scope,
                          String operation,
                          QueryOptions options,
                          QueryState state) {
        this.type = type;
        this.source = source;
        this.user = user;
        this.timestamp = timestamp;
        this.batch = batch;
        this.keyspace = keyspace;
        this.scope = scope;
        this.operation = operation;
        this.options = options;
        this.state = state;
    }

    String getLogString() {
        StringBuilder builder = new StringBuilder(100);
        builder.append("user:").append(user)
                .append("|host:").append(host)
                .append("|source:").append(source.getAddress());
        if (source.getPort() > 0) {
            builder.append("|port:").append(source.getPort());
        }

        builder.append("|timestamp:").append(timestamp)
                .append("|type:").append(type)
                .append("|category:").append(type.getCategory());

        if (batch != null) {
            builder.append("|batch:").append(batch);
        }
        if (StringUtils.isNotBlank(keyspace)) {
            builder.append("|ks:").append(keyspace);
        }
        if (StringUtils.isNotBlank(scope)) {
            builder.append("|scope:").append(scope);
        }
        if (StringUtils.isNotBlank(operation)) {
            String s = operation.replace('\r', ' ').replace('\n', ' ').replaceAll(" {2,}+", " ");
            builder.append("|operation:").append(s);

            System.out.println("LEI TEST [INFO] 打印 sql :" + s);
            System.out.println("LEI TEST [INFO] 操作类型:" + type.toString());

            String esNodeList = DatabaseDescriptor.getEsNodeList();

            System.out.println("LEI TEST [INFO] 打印节点列表：" + esNodeList);

            if (type.toString().equals("CREATE_TABLE")) {
                boolean syncEs = StringUtils.isBlank(DatabaseDescriptor.getSyncEsTable()) || !DatabaseDescriptor.getSyncEsTable().equals(keyspace+"."+scope)? true:false;
                logger.info("CREATE_TABLE 同步ES："+syncEs);
                System.out.println("CREATE_TABLE 同步ES："+syncEs);
                if (syncEs) {
                    HttpUtil.newCreateIndex(esNodeList, keyspace + "-" + scope);
                }
            }


            if (type.toString().equals("BATCH")) {
                String batchSql = s.replace("BEGIN BATCH", "").replace("APPLY BATCH;", "");
                String[] split = batchSql.split(";");
                for (int i = 0; i < split.length; i++) {
                    String sql = split[i].toLowerCase();
                    if (!StringUtils.isBlank(sql)) {
                        sql = sql + ";";
                        logger.info("BATCH 解析 CQL 语句:"+sql);
                        String matchSqlTableName = CassandraUtil.matchSqlTableName(sql.trim());
                        Boolean aBoolean = StringUtils.isBlank(DatabaseDescriptor.getSyncEsTable()) || !DatabaseDescriptor.getSyncEsTable().equals(keyspace+"."+matchSqlTableName)? true:false;
                        logger.info("BATCH 同步es："+aBoolean);
                        if (aBoolean) {
                            if (sql.indexOf("insert") > 0) {
                                Map<String, Object> maps = SqlToJson.sqlInsertToJosn(sql);
                                HttpUtil.bulkIndex(esNodeList, keyspace + "-" + matchSqlTableName, maps);
                            } else if (sql.indexOf("update") > 0) {
                                Map sqlMaps = SqlToJson.sqlUpdateToJson(sql);

                                Map<String, Object> updateSqlWhere = EsUtil.getUpdateSqlWhere(sql);
                                DataRsp<Object> dataRsp = HttpUtil.getSearch(esNodeList, keyspace + "-" + scope, updateSqlWhere);
                                List<Hites> hitesList = EsUtil.castList(dataRsp.getData(), Hites.class);
                                hitesList.stream().forEach(hites -> {
                                    Map<String, Object> source = hites.get_source();
                                    Map updateJson = EsUtil.mergeTwoMap(sqlMaps, source);
                                    HttpUtil.bulkUpdate(esNodeList, keyspace + "-" + matchSqlTableName, updateJson, hites.get_id());
                                });
                            } else if (sql.indexOf("delete") > 0) {
                                Map maps = SqlToJson.sqlDeleteToJson(sql);
                                HttpUtil.deleteData(esNodeList, keyspace + "-" + matchSqlTableName, maps);
                            }
                        }
                    }
                }
            }

            if (type.toString().equals("UPDATE")) {
                boolean syncEs = StringUtils.isBlank(DatabaseDescriptor.getSyncEsTable()) || !DatabaseDescriptor.getSyncEsTable().equals(keyspace+"."+scope)? true:false;
                logger.info("UPDATE 同步es："+syncEs);
                if (syncEs) {
                    if (s.toLowerCase(Locale.ROOT).contains("update")) {
                        Map sqlMaps = SqlToJson.sqlUpdateToJson(s);

                        Map<String, Object> updateSqlWhere = EsUtil.getUpdateSqlWhere(s);
                        DataRsp<Object> dataRsp = HttpUtil.getSearch(esNodeList, keyspace + "-" + scope, updateSqlWhere);
                        List<Hites> hitesList = EsUtil.castList(dataRsp.getData(), Hites.class);
                        hitesList.stream().forEach(hites -> {
                            Map<String, Object> source = hites.get_source();
                            Map updateJson = EsUtil.mergeTwoMap(sqlMaps, source);
                            HttpUtil.bulkUpdate(esNodeList, keyspace + "-" + scope, updateJson, hites.get_id());
                        });

                    } else {
                        Map<String, Object> maps = SqlToJson.sqlInsertToJosn(s);
                        System.out.println("LEI TEST [INFO][INSERT] 需要发送ES的数据:" + JSON.toJSONString(maps));
                        HttpUtil.bulkIndex(esNodeList, keyspace + "-" + scope, maps);
                    }
                }
            }


            if (type.toString().equals("DELETE")) {
                boolean syncEs = StringUtils.isBlank(DatabaseDescriptor.getSyncEsTable()) || !DatabaseDescriptor.getSyncEsTable().equals(keyspace+"."+scope)? true:false;
                logger.info("DELETE 同步es："+syncEs);
                if (syncEs) {
                    Map maps = SqlToJson.sqlDeleteToJson(s);
                    HttpUtil.deleteData(esNodeList, keyspace + "-" + scope, maps);
                }
            }

            if (type.toString().equals("DROP_TABLE")) {
                boolean syncEs = StringUtils.isBlank(DatabaseDescriptor.getSyncEsTable()) || !DatabaseDescriptor.getSyncEsTable().equals(keyspace+"."+scope)? true:false;
                logger.info("DROP TABLE 同步es："+syncEs);
                if (syncEs) {
                    HttpUtil.dropIndex(esNodeList, keyspace + "-" + scope);
                }
            }

        }
        return builder.toString();
    }


    public InetAddressAndPort getHost() {
        return host;
    }

    public InetAddressAndPort getSource() {
        return source;
    }

    public String getUser() {
        return user;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public AuditLogEntryType getType() {
        return type;
    }

    public UUID getBatch() {
        return batch;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getScope() {
        return scope;
    }

    public String getOperation() {
        return operation;
    }

    public QueryOptions getOptions() {
        return options;
    }

    public QueryState getState() {
        return state;
    }

    public static class Builder {
        private static final InetAddressAndPort DEFAULT_SOURCE;

        static {
            try {
                DEFAULT_SOURCE = InetAddressAndPort.getByNameOverrideDefaults("0.0.0.0", 0);
            } catch (UnknownHostException e) {

                throw new RuntimeException("failed to create default source address", e);
            }
        }

        private static final String DEFAULT_OPERATION = StringUtils.EMPTY;

        private AuditLogEntryType type;
        private InetAddressAndPort source;
        private String user;
        private long timestamp;
        private UUID batch;
        private String keyspace;
        private String scope;
        private String operation;
        private QueryOptions options;
        private QueryState state;

        public Builder(QueryState queryState) {
            state = queryState;

            ClientState clientState = queryState.getClientState();

            if (clientState != null) {
                if (clientState.getRemoteAddress() != null) {
                    InetSocketAddress addr = clientState.getRemoteAddress();
                    source = InetAddressAndPort.getByAddressOverrideDefaults(addr.getAddress(), addr.getPort());
                }

                if (clientState.getUser() != null) {
                    user = clientState.getUser().getName();
                }
                keyspace = clientState.getRawKeyspace();
            } else {
                source = DEFAULT_SOURCE;
                user = AuthenticatedUser.SYSTEM_USER.getName();
            }

            timestamp = currentTimeMillis();
        }

        public Builder(AuditLogEntry entry) {
            type = entry.type;
            source = entry.source;
            user = entry.user;
            timestamp = entry.timestamp;
            batch = entry.batch;
            keyspace = entry.keyspace;
            scope = entry.scope;
            operation = entry.operation;
            options = entry.options;
            state = entry.state;
        }

        public Builder setType(AuditLogEntryType type) {
            this.type = type;
            return this;
        }

        public Builder(AuditLogEntryType type) {
            this.type = type;
            operation = DEFAULT_OPERATION;
        }

        public Builder setUser(String user) {
            this.user = user;
            return this;
        }

        public Builder setBatch(UUID batch) {
            this.batch = batch;
            return this;
        }

        public Builder setTimestamp(long timestampMillis) {
            this.timestamp = timestampMillis;
            return this;
        }

        public Builder setKeyspace(QueryState queryState, @Nullable CQLStatement statement) {
            keyspace = statement != null && statement.getAuditLogContext().keyspace != null
                    ? statement.getAuditLogContext().keyspace
                    : queryState.getClientState().getRawKeyspace();
            return this;
        }

        public Builder setKeyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public Builder setKeyspace(CQLStatement statement) {
            this.keyspace = statement.getAuditLogContext().keyspace;
            return this;
        }

        public Builder setScope(CQLStatement statement) {
            this.scope = statement.getAuditLogContext().scope;
            return this;
        }

        public Builder setOperation(String operation) {
            this.operation = operation;
            return this;
        }

        public void appendToOperation(String str) {
            if (StringUtils.isNotBlank(str)) {
                if (operation.isEmpty())
                    operation = str;
                else
                    operation = operation.concat("; ").concat(str);
            }
        }

        public Builder setOptions(QueryOptions options) {
            this.options = options;
            return this;
        }

        public AuditLogEntry build() {
            timestamp = timestamp > 0 ? timestamp : currentTimeMillis();
            return new AuditLogEntry(type, source, user, timestamp, batch, keyspace, scope, operation, options, state);
        }
    }
}

