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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.audit.es.BinaryStringConverteUtil;
import org.apache.cassandra.audit.es.HttpUtil;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.AuthenticationStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

public class QueryEvents {
    private static final Logger logger = LoggerFactory.getLogger(QueryEvents.class);
    private static final NoSpamLogger noSpam1m = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    public static final QueryEvents instance = new QueryEvents();

    private final Set<Listener> listeners = new CopyOnWriteArraySet<>();

    @VisibleForTesting
    public int listenerCount() {
        return listeners.size();
    }

    public void registerListener(Listener listener) {
        listeners.add(listener);
    }

    public void unregisterListener(Listener listener) {
        listeners.remove(listener);
    }

    public void notifyQuerySuccess(CQLStatement statement,
                                   String query,
                                   QueryOptions options,
                                   QueryState state,
                                   long queryTime,
                                   Message.Response response) {
        try {
            final String maybeObfuscatedQuery = listeners.size() > 0 ? maybeObfuscatePassword(statement, query) : query;

            String cql = maybeObfuscatedQuery;

            for (Listener listener : listeners)
                listener.querySuccess(statement, cql, options, state, queryTime, response);
        } catch (Throwable t) {
            noSpam1m.error("Failed notifying listeners", t);
            JVMStabilityInspector.inspectThrowable(t);
        }
    }

    public void notifyQueryFailure(CQLStatement statement,
                                   String query,
                                   QueryOptions options,
                                   QueryState state,
                                   Exception cause) {
        try {
            final String maybeObfuscatedQuery = listeners.size() > 0 ? maybeObfuscatePassword(statement, query) : query;
            for (Listener listener : listeners)
                listener.queryFailure(statement, maybeObfuscatedQuery, options, state, cause);
        } catch (Throwable t) {
            noSpam1m.error("Failed notifying listeners", t);
            JVMStabilityInspector.inspectThrowable(t);
        }
    }

    public void notifyExecuteSuccess(CQLStatement statement,
                                     String query,
                                     QueryOptions options,
                                     QueryState state,
                                     long queryTime,
                                     Message.Response response) {
        try {
            final String maybeObfuscatedQuery = listeners.size() > 0 ? maybeObfuscatePassword(statement, query) : query;
            String cql = maybeObfuscatedQuery;
            System.out.println("------------notifyExecuteSuccess 找数据------------");
            if (cql.contains("?")) {
                boolean syncEs = StringUtils.isBlank(DatabaseDescriptor.getSyncEsTable()) || !DatabaseDescriptor.getSyncEsTable().equals(statement.getAuditLogContext().keyspace + "." + statement.getAuditLogContext().scope) ? true : false;
                Map<String, Object> maps = new HashMap<>();
                for (int i = 0; i < statement.getBindVariables().size(); i++) {

                    ColumnSpecification cs = statement.getBindVariables().get(i);
                    String boundName = cs.name.toString();
                    String boundValue = cs.type.asCQL3Type().toCQLLiteral(options.getValues().get(i), options.getProtocolVersion());
                    System.out.println("字段名字:" + boundName + ";类型:" + cs.type.asCQL3Type());
                    System.out.println("key:" + boundName);
                    System.out.println("value:" + boundValue);
                    System.out.println("Interned:"+cs.name);
                    // Opensearch 数据里不能有特殊字符 \ 和 ", 过滤掉
                    boundValue = boundValue.replace("\\", "");
                    boundValue = boundValue.replace("\"", "");
                    maps.put(boundName, boundValue);

                }
                if (syncEs) {
                    HttpUtil.bulkIndex("http://127.0.0.1:9200", statement.getAuditLogContext().keyspace + "-" + statement.getAuditLogContext().scope, maps);
                }

            }

            if (cql.contains(":")) {
                boolean syncEs = StringUtils.isBlank(DatabaseDescriptor.getSyncEsTable()) || !DatabaseDescriptor.getSyncEsTable().equals(statement.getAuditLogContext().keyspace + "." + statement.getAuditLogContext().scope) ? true : false;
                Map<String, Object> maps = new HashMap<>();
                for (int i = 0; i < statement.getBindVariables().size(); i++) {

                    ColumnSpecification cs = statement.getBindVariables().get(i);
                    String boundName = cs.name.toString();
                    String boundValue = cs.type.asCQL3Type().toCQLLiteral(options.getValues().get(i), options.getProtocolVersion());
                    System.out.println("字段名字:" + boundName + ";类型:" + cs.type.asCQL3Type());
                    System.out.println("key:" + boundName);
                    System.out.println("value:" + boundValue);
                    System.out.println("Interned:"+cs.name.isInterned());
                    // Opensearch 数据里不能有特殊字符 \ 和 ", 过滤掉
                    boundValue = boundValue.replace("\\", "");
                    boundValue = boundValue.replace("\"", "");
                    maps.put(boundName, boundValue);

                }
                if (syncEs) {
                    HttpUtil.bulkIndex("http://127.0.0.1:9200", statement.getAuditLogContext().keyspace + "-" + statement.getAuditLogContext().scope, maps);
                }

            }
            System.out.println("------------------------------");


            for (Listener listener : listeners)
                listener.executeSuccess(statement, maybeObfuscatedQuery, options, state, queryTime, response);
        } catch (Throwable t) {
            noSpam1m.error("Failed notifying listeners", t);
            JVMStabilityInspector.inspectThrowable(t);
        }
    }

    public void notifyExecuteFailure(QueryHandler.Prepared prepared,
                                     QueryOptions options,
                                     QueryState state,
                                     Exception cause) {
        CQLStatement statement = prepared != null ? prepared.statement : null;
        String query = prepared != null ? prepared.rawCQLStatement : null;
        try {
            final String maybeObfuscatedQuery = listeners.size() > 0 ? maybeObfuscatePassword(statement, query) : query;
            for (Listener listener : listeners)
                listener.executeFailure(statement, maybeObfuscatedQuery, options, state, cause);
        } catch (Throwable t) {
            noSpam1m.error("Failed notifying listeners", t);
            JVMStabilityInspector.inspectThrowable(t);
        }
    }

    public void notifyBatchSuccess(BatchStatement.Type batchType,
                                   List<? extends CQLStatement> statements,
                                   List<String> queries,
                                   List<List<ByteBuffer>> values,
                                   QueryOptions options,
                                   QueryState state,
                                   long queryTime,
                                   Message.Response response) {
        try {
            System.out.println("---------------notifyBatchSuccess");
            for (Listener listener : listeners)
                listener.batchSuccess(batchType, statements, queries, values, options, state, queryTime, response);
        } catch (Throwable t) {
            noSpam1m.error("Failed notifying listeners", t);
            JVMStabilityInspector.inspectThrowable(t);
        }
    }

    public void notifyBatchFailure(List<QueryHandler.Prepared> prepared,
                                   BatchStatement.Type batchType,
                                   List<Object> queryOrIdList,
                                   List<List<ByteBuffer>> values,
                                   QueryOptions options,
                                   QueryState state,
                                   Exception cause) {
        if (hasListeners()) {
            List<CQLStatement> statements = new ArrayList<>(queryOrIdList.size());
            List<String> queries = new ArrayList<>(queryOrIdList.size());
            if (prepared != null) {
                prepared.forEach(p -> {
                    statements.add(p.statement);
                    queries.add(p.rawCQLStatement);
                });
            }
            try {
                for (Listener listener : listeners)
                    listener.batchFailure(batchType, statements, queries, values, options, state, cause);
            } catch (Throwable t) {
                noSpam1m.error("Failed notifying listeners", t);
                JVMStabilityInspector.inspectThrowable(t);
            }
        }
    }

    public void notifyPrepareSuccess(Supplier<QueryHandler.Prepared> preparedProvider,
                                     String query,
                                     QueryState state,
                                     long queryTime,
                                     ResultMessage.Prepared response) {
        if (hasListeners()) {
            QueryHandler.Prepared prepared = preparedProvider.get();
            if (prepared != null) {
                try {
                    final String maybeObfuscatedQuery = listeners.size() > 0 ? maybeObfuscatePassword(prepared.statement, query) : query;

                    for (Listener listener : listeners)
                        listener.prepareSuccess(prepared.statement, maybeObfuscatedQuery, state, queryTime, response);
                } catch (Throwable t) {
                    noSpam1m.error("Failed notifying listeners", t);
                    JVMStabilityInspector.inspectThrowable(t);
                }
            } else {
                // this means that queryHandler.prepare was successful, but then immediately after we can't find the prepared query in the cache, should be very rare
                notifyPrepareFailure(null, query, state, new RuntimeException("Successfully prepared, but could not find prepared statement for " + response.statementId));
            }
        }
    }

    public void notifyPrepareFailure(@Nullable CQLStatement statement, String query, QueryState state, Exception cause) {
        try {
            final String maybeObfuscatedQuery = listeners.size() > 0 ? maybeObfuscatePassword(statement, query) : query;
            for (Listener listener : listeners)
                listener.prepareFailure(statement, maybeObfuscatedQuery, state, cause);
        } catch (Throwable t) {
            noSpam1m.error("Failed notifying listeners", t);
            JVMStabilityInspector.inspectThrowable(t);
        }
    }

    private String maybeObfuscatePassword(CQLStatement statement, String query) {
        // Statement might be null as side-effect of failed parsing, originates from QueryMessage#execute
        if (statement == null)
            return PasswordObfuscator.obfuscate(query);

        if (statement instanceof AuthenticationStatement)
            return ((AuthenticationStatement) statement).obfuscatePassword(query);

        return query;
    }

    public boolean hasListeners() {
        return !listeners.isEmpty();
    }

    public static interface Listener {
        default void querySuccess(CQLStatement statement,
                                  String query,
                                  QueryOptions options,
                                  QueryState state,
                                  long queryTime,
                                  Message.Response response) {
        }

        default void queryFailure(@Nullable CQLStatement statement,
                                  String query,
                                  QueryOptions options,
                                  QueryState state,
                                  Exception cause) {
        }

        default void executeSuccess(CQLStatement statement,
                                    String query,
                                    QueryOptions options,
                                    QueryState state,
                                    long queryTime,
                                    Message.Response response) {
        }

        default void executeFailure(@Nullable CQLStatement statement,
                                    @Nullable String query,
                                    QueryOptions options,
                                    QueryState state,
                                    Exception cause) {
        }

        default void batchSuccess(BatchStatement.Type batchType,
                                  List<? extends CQLStatement> statements,
                                  List<String> queries,
                                  List<List<ByteBuffer>> values,
                                  QueryOptions options,
                                  QueryState state,
                                  long queryTime,
                                  Message.Response response) {
        }

        default void batchFailure(BatchStatement.Type batchType,
                                  List<? extends CQLStatement> statements,
                                  List<String> queries,
                                  List<List<ByteBuffer>> values,
                                  QueryOptions options,
                                  QueryState state,
                                  Exception cause) {
        }

        default void prepareSuccess(CQLStatement statement,
                                    String query,
                                    QueryState state,
                                    long queryTime,
                                    ResultMessage.Prepared response) {
        }

        default void prepareFailure(@Nullable CQLStatement statement,
                                    String query,
                                    QueryState state,
                                    Exception cause) {
        }
    }
}
