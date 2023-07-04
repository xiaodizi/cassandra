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

package org.apache.second.esclient;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import jakarta.json.Json;
import jakarta.json.stream.JsonParser;
import org.apache.second.Utils;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch._types.mapping.*;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.*;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.search.SourceConfigParam;
import org.opensearch.client.opensearch.indices.*;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;


public class ElasticIndex {

    private static final Logger logger = LoggerFactory.getLogger(ElasticIndex.class);

    private final List<String> partitionKeysNames;
    private final List<String> clusteringColumnsNames;
    private final boolean hasClusteringColumns;

    private final OpenSearchClient client;

    private static final String ES_SOURCE = "_source";

    public ElasticIndex(@Nonnull OpenSearchClient client, @Nonnull List<String> partitionKeysNames, @Nonnull List<String> clusteringColumnsNames) {
        this.partitionKeysNames = partitionKeysNames;
        this.clusteringColumnsNames = clusteringColumnsNames;
        this.hasClusteringColumns = !clusteringColumnsNames.isEmpty();
        this.client = client;
    }

    public Boolean newIndex(String indexName, Map<String, Map<String, String>> fields, Integer refreshSecond) throws IOException {

        int dataNodes = client.cluster().health().numberOfDataNodes();

        HashMap<String, Object> map1 = new HashMap<>();
        for (String key : fields.keySet()) {
            HashMap<String, Object> map2 = new HashMap<>();
            Map<String, String> filedmap = fields.get(key);
            for (String key2 : filedmap.keySet()) {
                String value2 = filedmap.get(key2);
                if (key2.equals("type") || key2.equals("analyzer")) {
                    map2.put(key2, value2);
                    if (value2.equals("text")) {
                        Map<String, Object> childMap = new HashMap<>();
                        childMap.put("type", "keyword");
                        childMap.put("ignore_above", 256);
                        Map<String, Object> keywordMap = new HashMap<>();
                        keywordMap.put("keyword", childMap);
                        map2.put("fields", keywordMap);
                    }

                }
            }
            map1.put(key, map2);
        }

        HashMap<String, Object> mappings = new HashMap<>();

        mappings.put("properties", map1);

        JsonpMapper mapper = client._transport().jsonpMapper();
        JsonParser parser = Json.createParser(new StringReader(JSON.toJSONString(mappings)));

        CreateIndexRequest index = new CreateIndexRequest.Builder().index(indexName).settings(new IndexSettings.Builder().numberOfShards(String.valueOf(dataNodes)).numberOfReplicas(String.valueOf(dataNodes - 1)).refreshInterval(new Time.Builder().time(refreshSecond.toString() + "s").build()).build()).mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper)).build();
        CreateIndexResponse createIndexResponse = client.indices().create(index);
        return createIndexResponse.acknowledged();
    }


    public Boolean isExistsIndex(String indexName) throws IOException {
        ExistsRequest request = new ExistsRequest.Builder().index(indexName).build();
        BooleanResponse exists = client.indices().exists(request);
        return exists.value();
    }

    public Boolean indexData(Map<String, Object> maps, String indexName, String primaryKeyValue) throws IOException {
        JSONObject object = JSONObject.parseObject(JSON.toJSONString(maps));
        IndexRequest<Object> indexRequest = new IndexRequest.Builder<Object>().index(indexName).id(primaryKeyValue).document(object).build();
        IndexResponse index = client.index(indexRequest);
        if (index.shards().successful().intValue() != 0) {
            return true;
        }
        return false;
    }

    public Boolean bulkData(Map<String, Object> maps, String indexName, String primaryKeyValue) {

        JSONObject object = JSONObject.parseObject(JSON.toJSONString(maps));
        List<BulkOperation> operationList = new ArrayList<>();
        BulkOperation operation = new BulkOperation.Builder().index(new IndexOperation.Builder<>().id(primaryKeyValue).index(indexName).document(object).build()).build();
        try {
            operationList.add(operation);
            BulkRequest request = new BulkRequest.Builder().index(indexName).operations(operationList).source(new SourceConfigParam.Builder().fetch(true).build()).build();
            BulkResponse bulk = client.bulk(request);
            if (bulk.errors() == false) {
                System.out.println("这里............");
                return true;
            } else {
                logger.debug("Bluk 返回结果:" + bulk.errors() + ";");
                bulk.items().stream().forEach(i -> {
                    logger.debug(" Bluk Data ID:" + i.id() + ";错误原因:" + i.error().reason());
                });
            }
        } catch (Exception e) {
            logger.error("Bluk 数据过程有错误:", e);
        }
        return false;
    }


    public Boolean delData(String indexName, String primaryKeyValue) throws IOException {

        DeleteRequest request = new DeleteRequest.Builder().index(indexName).id(primaryKeyValue).build();
        DeleteResponse delete = client.delete(request);
        if (delete.shards().successful().intValue() != 0) {
            return true;
        }
        return false;
    }

    public void dropIndex(String indexName) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest.Builder().index(indexName).build();
        DeleteIndexResponse delete = client.indices().delete(request);
        logger.info("删除索引返回:" + delete.acknowledged());
    }


    public boolean refreshData(String indexName) throws IOException {
        RefreshRequest request = new RefreshRequest.Builder().index(indexName).build();
        RefreshResponse refresh = client.indices().refresh(request);
        if (refresh.shards().failed().intValue() == 0) {
            return true;
        }
        return false;
    }

    public SearchResult searchData(String indexName, Map<String, Object> mappings) throws IOException {
        Map<String, Object> query = (Map<String, Object>) mappings.get("query");
        String json = "";
        if (query.get("bool") != null) {
            json = parseEsBool(query);
        } else {
            json = parseEsQuery(query);
        }
        JsonpMapper mapper = client._transport().jsonpMapper();
        JsonParser parser = Json.createParser(new StringReader(json));
        SearchRequest searchRequest = new SearchRequest.Builder().index(indexName).query(Query._DESERIALIZER.deserialize(parser, mapper)).build();

        SearchResponse<JSONObject> search = client.search(searchRequest, JSONObject.class);

        List<String> primaryKeys;

        if (hasClusteringColumns) {
            primaryKeys = new ArrayList<>(partitionKeysNames.size() + clusteringColumnsNames.size());
            primaryKeys.addAll(partitionKeysNames);
            primaryKeys.addAll(clusteringColumnsNames);
        } else {
            primaryKeys = partitionKeysNames;
        }


        int pkSize = primaryKeys.size();

        List<SearchResultRow> rowList = new ArrayList<>();
        search.hits().hits().stream().forEach(doc -> {
            String[] primaryKey = new String[pkSize];
            int keyNb = 0;

            for (String keyName : primaryKeys) {
                String value = doc.source().get(keyName).toString();

                if (value == null) {
                    continue;
                } else {
                    primaryKey[keyNb] = value;
                }
                keyNb++;
            }

            SearchResultRow searchResultRow = new SearchResultRow(primaryKey, doc.source());
            rowList.add(searchResultRow);
        });
        return new SearchResult(rowList);
    }


    // 普通查询
    private static String parseEsQuery(Map<String, Object> queryMps) {
        Map<String, Object> aggsMaps = new HashMap<>();
        Map<String, Object> fieldMaps = new HashMap<>();
        Map<String, Object> whereMaps = new HashMap<>();
        for (String m : queryMps.keySet()) {
            if (!m.equals("type") && !m.equals("field")) {
                whereMaps.put(m, queryMps.get(m));
            }
        }
        Object value = queryMps.get("value");
        if (value == null) {
            fieldMaps.put(queryMps.get("field").toString(), whereMaps);
        } else {
            fieldMaps.put(queryMps.get("field").toString(), value);
        }
        aggsMaps.put(queryMps.get("type").toString(), fieldMaps);
        return JSON.toJSONString(aggsMaps);
    }

    // bool 查询
    private static String parseEsBool(Map<String, Object> boolMps) {
        Map<String, Object> aggsMaps = new HashMap<>();
        Map<String, List<Map<String, Object>>> filter = (Map<String, List<Map<String, Object>>>) boolMps.get("bool");
        Map<String, Object> boolWhere = new HashMap<>();
        for (String key : filter.keySet()) {
            List<Object> list = new ArrayList<>();
            List<Map<String, Object>> mapList = filter.get(key);
            mapList.stream().forEach(mps -> {
                String s = parseEsQuery(mps);
                Map map = JSONObject.parseObject(s, Map.class);
                list.add(map);
            });
            boolWhere.put(key, list);
        }
        aggsMaps.put("bool", boolWhere);
        return JSON.toJSONString(aggsMaps);
    }


    public static void main(String[] args) {
        String json = "{\n" + "   query: {bool:{filter:[{type: \"range\", field: \"time\", gte: \"2014-04-25\", lte: \"2014-05-21\"},{type: \"match_phrase\", field: \"body\", value: \"123456\"}]}}\n" + "}";
        Map map = JSONObject.parseObject(Utils.pattern(json), Map.class);
        Map<String, Object> query = (Map<String, Object>) map.get("query");

        if (query.get("bool") != null) {
            System.out.println("bool 查询");
            String s = parseEsBool(query);
            System.out.println(s);
        } else {
            System.out.println("query 普通查询");
            String s = parseEsQuery(query);
            System.out.println(s);
        }
    }


}
