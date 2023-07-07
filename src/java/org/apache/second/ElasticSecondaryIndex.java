/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains some code from Elasticsearch (http://www.elastic.co)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.second;


import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Stopwatch;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.second.esclient.ElasticIndex;
import org.apache.second.esclient.EsPartitionIterator;
import org.apache.second.esclient.SearchResult;
import org.apache.second.esclient.SearchResultRow;
import org.apache.second.indexers.EsIndexer;
import org.apache.second.indexers.NoOpPartitionIterator;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;


public class ElasticSecondaryIndex implements Index, INotificationConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSecondaryIndex.class);


//    final HttpHost[] hosts = new HttpHost[]{
//            new HttpHost(DatabaseDescriptor.getRpcAddress().getHostAddress(), 9200, "http")
//    };

    // 测试地址
    final HttpHost[] hosts = new HttpHost[]{
            new HttpHost("192.168.184.88", 9200, "http")
    };

    // Initialize the client with SSL and TLS enabled
    final RestClient restClient = RestClient
            .builder(hosts)
            .build();

    OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    OpenSearchClient client = new OpenSearchClient(transport);


    public final ColumnFamilyStore baseCfs;
    public final IndexMetadata config;

    private static final String UPDATE = "#update#";
    private static final String GET_MAPPING = "#get_mapping#";
    private static final String PUT_MAPPING = "#put_mapping#";

    private final SecureRandom random = new SecureRandom();

    // 索引名称
    public final String index_name;

    public final TableMetadata metadata;
    // keyspace 名字
    public final String ksName;
    // 表名
    public final String cfName;
    // 还是个表名
    public final String idxName;


    public final Map<String, Map<String, String>> schema;


    private final ElasticIndex elasticIndex;
    private List<String> partitionKeysNames;
    private List<String> clusteringColumnsNames;
    private boolean hasClusteringColumns;
    public final String indexColumnName;

    public Integer refreshSecond = 1;


    public ElasticSecondaryIndex(ColumnFamilyStore baseCfs, IndexMetadata config) {
        this.baseCfs = baseCfs;
        this.config = config;
        this.index_name = baseCfs.keyspace.getName() + "." + baseCfs.name;

        this.metadata = this.baseCfs.metadata();

        this.ksName = metadata.keyspace;
        this.cfName = metadata.name;
        this.idxName = config.name;


        indexColumnName = unQuote(config.options.get(IndexTarget.TARGET_OPTION_NAME));

        try {
            partitionKeysNames = Collections.unmodifiableList(Utils.getPartitionKeyNames(baseCfs.metadata()));
            clusteringColumnsNames = Collections.unmodifiableList(Utils.getClusteringColumnsNames(baseCfs.metadata()));
        } catch (CharacterCodingException e) {
            e.printStackTrace();
        }

        hasClusteringColumns = !clusteringColumnsNames.isEmpty();


        elasticIndex=new ElasticIndex(client,partitionKeysNames,clusteringColumnsNames);

        Map<String, String> options = config.options;
        refreshSecond = options.get("refresh_seconds") == null ? 5 : Integer.valueOf(options.get("refresh_seconds"));
        String schema = options.get("schema");
        Map<String, Map<String, String>> filedes = (Map<String, Map<String, String>>) JSONObject.parseObject(Utils.pattern(schema), Map.class).get("fields");
        this.schema = filedes;
        try {
            if (!elasticIndex.isExistsIndex(index_name)) {
                elasticIndex.newIndex(this.index_name, filedes,refreshSecond);
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("索引创建异常:", e);
        }
    }


    @Override
    public Callable<?> getInitializationTask() {
        System.out.println("------------lei test1----------");
        return null;
    }

    @Override
    public IndexMetadata getIndexMetadata() {
        return config;
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata) {
        System.out.println("------------lei test3----------");
        return null;
    }

    @Override
    public void register(IndexRegistry registry) {
        registry.registerIndex(this);
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable() {
        return Optional.empty();
    }

    @Override
    public Callable<?> getBlockingFlushTask() {
        System.out.println("------------lei test6----------");
        return null;
    }

    @Override
    public Callable<?> getInvalidateTask() {
        System.out.println("------------lei test7----------");
        try {
            elasticIndex.dropIndex(this.index_name);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Callable<?> getTruncateTask(long truncatedAt) {
        System.out.println("------------lei test8----------");
        return null;
    }

    @Override
    public boolean shouldBuildBlocking() {
        System.out.println("------------lei test9----------");
        return false;
    }

    @Override
    public boolean dependsOn(ColumnMetadata column) {
        System.out.println("------------lei test10----------");
        return false;
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator) {
        System.out.println("------------lei test11----------");
        return false;
    }

    @Override
    public AbstractType<?> customExpressionValueType() {
        System.out.println("------------lei test12----------");
        return UTF8Type.instance;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter) {
        System.out.println("------------lei test13----------");
        return RowFilter.NONE;
    }

    @Override
    public long getEstimatedResultRows() {
        System.out.println("------------lei test14----------");
        return -Math.abs(random.nextLong());
    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException {

    }

    @Override
    public void validate(ReadCommand command) throws InvalidRequestException {
        String queryString = Utils.queryString(command);
        System.out.println("------------lei test15----------");
        if (!queryString.startsWith(UPDATE) && !queryString.startsWith(GET_MAPPING) && !queryString.startsWith(PUT_MAPPING)) {
            //esIndex.validate(queryString);
        }
    }

    @Override
    public Indexer indexerFor(DecoratedKey key, RegularAndStaticColumns columns, int nowInSec, WriteContext ctx, IndexTransaction.Type transactionType) {
        return new EsIndexer(this, key, nowInSec, false);
    }

    @Override
    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command) {
        System.out.println("------------lei test17----------");
        return NoOpPartitionIterator.INSTANCE;
    }

    @Override
    public Searcher searcherFor(ReadCommand command) {
        System.out.println("------------lei test18----------");
        return executionController -> search(executionController,command);
    }

    @Override
    public void handleNotification(INotification notification, Object sender) {
        System.out.println("------------lei test----------");
    }


    @Nullable
    public void index(@Nonnull DecoratedKey decoratedKey, @Nonnull Row newRow, @Nullable Row oldRow, int nowInSec) {
        final Stopwatch time = Stopwatch.createStarted();
        String primaryKey = decoratedKey.getPrimaryKey(this.baseCfs.metadata());
        String primaryKeyValue = decoratedKey.getPrimaryKeyValue(this.baseCfs.metadata()).replace("'","");
        try {
            Map<String, Object> maps = Utils.toMaps(newRow,this.schema);
            maps.put(primaryKey, primaryKeyValue);
            HashMap<String, Object> jsonMap = new HashMap<>();
            for (String key : this.schema.keySet()) {
                jsonMap.put(key, maps.get(key));
            }
            elasticIndex.indexData(jsonMap, index_name, primaryKeyValue);
        } catch (Exception e) {
           logger.error("Index "+index_name+" data Exception:",e);
        }

        logger.info("Index {} data  took {} ms", index_name, time.elapsed(TimeUnit.MILLISECONDS));
    }

    @Nullable
    public void delete(DecoratedKey decoratedKey) {
        final Stopwatch time = Stopwatch.createStarted();
        String primaryKeyValue = decoratedKey.getPrimaryKeyValue(baseCfs.metadata());
        try {
            elasticIndex.delData(index_name, primaryKeyValue);
        } catch (Exception e) {
            logger.error("delete data Exception:",e);
        }
        logger.info("delete data 结果，primaryKeyValue："+primaryKeyValue+" took {}ms",time.elapsed(TimeUnit.MILLISECONDS));
    }



    @Nonnull
    public UnfilteredPartitionIterator search(ReadExecutionController controller,ReadCommand command) {
        final Stopwatch time = Stopwatch.createStarted();
        final String queryString = Utils.queryString(command);
        Map map = JSONObject.parseObject(Utils.pattern(queryString), Map.class);

        System.out.println("bool查询条件:"+map);

        boolean refresh=map.get("refresh")==null ? false:Boolean.parseBoolean(map.get("refresh").toString());

        if (refresh){
            try {
                boolean b = elasticIndex.refreshData(index_name);
                logger.info("refresh data 结果:"+b);
            } catch (IOException e) {
                logger.error("refresh data faild:",e);
            }
        }

        final String searchId = UUID.randomUUID().toString();

        SearchResult searchResult = null;
        SearchResultRow searchResultRow=null;
        try {
            searchResult = elasticIndex.searchData(index_name, map);
            searchResultRow = searchResult.items.get((searchResult.items.size() - 1));
            searchResult.items.remove((searchResult.items.size()-1));
            fillPartitionAndClusteringKeys(searchResult.items);
        } catch (IOException e) {
            logger.error("query data faild:",e);
        }
        logger.info("{} select data took {}ms", index_name, time.elapsed(TimeUnit.MILLISECONDS));
        return new EsPartitionIterator(this, searchResult,partitionKeysNames, command, searchId,searchResultRow.docMetadata);
    }



    public void fillPartitionAndClusteringKeys(List<SearchResultRow> searchResultRows) {
        for (SearchResultRow searchResultRow : searchResultRows) {
            String[] rawKey = searchResultRow.primaryKey;
            final String[] partitionKeys;
            final String[] clusteringKeys;

            if (hasClusteringColumns) {
                clusteringKeys = new String[clusteringColumnsNames.size()];
                partitionKeys = new String[partitionKeysNames.size()];

                int pkPos = 0;
                int ckPos = 0;
                for (String key : rawKey) {
                    if (pkPos < partitionKeysNames.size()) {
                        partitionKeys[pkPos] = key;
                    } else {
                        clusteringKeys[ckPos] = key;
                        ckPos++;
                    }
                    pkPos++;
                }
            } else {
                partitionKeys = rawKey;
                clusteringKeys = null;
            }

            searchResultRow.partitionKey = Utils.getPartitionKeys(partitionKeys, baseCfs.metadata());
            searchResultRow.clusteringKeys = clusteringKeys;
        }
    }

    @Nonnull
    static String unQuote(@Nonnull String string) {
        return string.replaceAll("\"", "");
    }


}