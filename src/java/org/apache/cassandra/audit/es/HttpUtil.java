package org.apache.cassandra.audit.es;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import okhttp3.*;
import org.apache.cassandra.audit.es.common.ErrorEnum;
import org.apache.cassandra.audit.es.dto.EsClusterDto;
import org.apache.cassandra.audit.es.dto.EsResDto;
import org.apache.cassandra.audit.es.dto.Hites;
import org.apache.cassandra.audit.es.res.DataRsp;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpUtil {

    private static final Logger logger = LoggerFactory.getLogger(HttpUtil.class);


    public static DataRsp getClusterHealth() {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        EsClusterDto esClusterDto = null;
        try {

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/_cluster/health")
                    .build();
            Response response = client.newCall(request).execute();

            String bodyMessage = response.body().string();

            System.out.println("获取集群健康状态，返回：code:" + response.code() + "; 返回内容:" + bodyMessage);
            if (response.code() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.code())
                        .message(response.message())
                        .build();
            }
            esClusterDto = JSONObject.parseObject(bodyMessage, EsClusterDto.class);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.builder()
                .code(ErrorEnum.SUCCESS.code)
                .message(ErrorEnum.SUCCESS.message)
                .data(esClusterDto.getNumber_of_nodes())
                .build();
    }


    public static DataRsp newCreateIndex(String indexName) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        DataRsp clusterHealth = getClusterHealth();
        int numSharedNodes = Integer.parseInt(clusterHealth.getData().toString());

        try {
            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, "{\n  \"settings\":{\n    \"number_of_shards\":" + numSharedNodes + ",\n    \"number_of_replicas\":" + (numSharedNodes - 1) + "\n  }\n}");
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/" + indexName)
                    .method("PUT", body)
                    .build();
            Response response = client.newCall(request).execute();

            System.out.println("创建索引返回：code:" + response.code() + "; 返回内容:" + response.body().string());
            if (response.code() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.code())
                        .message(response.message())
                        .build();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }


    public static DataRsp createIndex(String indexName, String json, String id) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, json);
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/" + indexName + "/_doc/" + id)
                    .method("PUT", body)
                    .addHeader("Content-Type", "application/json")
                    .build();
            Response response = client.newCall(request).execute();


            System.out.println("Insert 数据返回：code:" + response.code() + "; 返回内容:" + response.body().string());

            if (response.code() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.code())
                        .message(response.message())
                        .build();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return DataRsp.getError200();
    }


    public static DataRsp bulkIndex(String indexName, Map<String, Object> maps, String keyValue) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";

        try {
            String bulkApiJson = EsUtil.getBulkCreateApiJson(maps, keyValue);
            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, bulkApiJson);
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/" + indexName + "/_bulk")
                    .method("POST", body)
                    .addHeader("Content-Type", "application/json")
                    .build();
            Response response = client.newCall(request).execute();

            System.out.println("Bulk 数据返回：code:" + response.code() + "; 返回内容:" + response.body().string());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }


    public static DataRsp bulkUpdate(String indexName, Map<String, Object> maps, String docId) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {
            String bulkApiJson = EsUtil.getBulkUpdateApiJson(maps, docId);

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, bulkApiJson);
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/" + indexName + "/_bulk")
                    .method("POST", body)
                    .addHeader("Content-Type", "application/json")
                    .build();
            Response response = client.newCall(request).execute();

            System.out.println("Bulk 数据返回：code:" + response.code() + "; 返回内容:" + response.body().string());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }


    public static DataRsp<Object> getSearch(String indexName, Map<String, Object> maps) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        List<Hites> hitesList = new ArrayList<>();
        try {

            String requestJson = EsUtil.getDslQueryJson(maps);
            System.out.println("查询 DSL：" + requestJson);
            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, requestJson);
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/" + indexName + "/_search")
                    .method("POST", body)
                    .addHeader("Content-Type", "application/json")
                    .build();
            Response response = client.newCall(request).execute();

            String respBody = response.body().string();

            System.out.println("结果1：" + response.code());
            EsResDto esResDto = JSONObject.parseObject(respBody, EsResDto.class);
            System.out.println("结果2：" + JSON.toJSONString(esResDto));
            hitesList = esResDto.getHits().getHits();

            if (response.code() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.code())
                        .message(response.message())
                        .data(hitesList)
                        .build();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return DataRsp.builder().code(ErrorEnum.SUCCESS.code)
                .message(ErrorEnum.SUCCESS.message)
                .data(hitesList)
                .build();
    }


    public static DataRsp<Object> getIndex(String indexName, String id) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("text/plain");
            RequestBody body = RequestBody.create(mediaType, "");
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/" + indexName + "/_doc/" + id)
                    .method("GET", body)
                    .build();
            Response response = client.newCall(request).execute();


            if (ErrorEnum.SUCCESS.code != response.code()) {
                return DataRsp.builder()
                        .code(response.code())
                        .message(response.message())
                        .build();
            }
        } catch (Exception e) {
            System.out.print("LEI TEST ERROR:");
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }


    public static DataRsp deleteData(String indexName, Map maps) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {
            String requestJson = EsUtil.getDslQueryJson(maps);
            System.out.println("删除 DSL：" + requestJson);

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, requestJson);
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/" + indexName + "/_delete_by_query?slices=auto&conflicts=proceed&wait_for_completion=false")
                    .method("POST", body)
                    .addHeader("Content-Type", "application/json")
                    .build();
            Response response = client.newCall(request).execute();

            System.out.println("删除返回：code:" + response.code() + "; 返回内容:" + response.body().string());
            if (ErrorEnum.SUCCESS.code != response.code()) {
                return DataRsp.builder()
                        .code(response.code())
                        .message(response.message())
                        .build();
            }
        } catch (Exception e) {
            System.out.print("LEI TEST ERROR:");
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }


    public static DataRsp dropIndex(String indexName) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("text/plain");
            RequestBody body = RequestBody.create(mediaType, "");
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/" + indexName)
                    .method("DELETE", body)
                    .build();
            Response response = client.newCall(request).execute();

            System.out.println("删除索引返回：code:" + response.code() + "; 返回内容:" + response.body().string());
            if (ErrorEnum.SUCCESS.code != response.code()) {
                return DataRsp.builder()
                        .code(response.code())
                        .message(response.message())
                        .build();
            }

        } catch (Exception e) {
            System.out.print("LEI TEST ERROR:");
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }


    public static DataRsp createCassandraMetadata(String keyspace, String table, boolean isSyncEs) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        if (!isExitsCassandraMetadata()) {
            newCreateIndex(".cassandra_metadata");
        }

        try {
            String json = "{\n" +
                    "  \"keyspace\":\"" + keyspace + "\",\n" +
                    "  \"table\":\"" + table + "\",\n" +
                    "  \"isSyncEs\":" + isSyncEs + ",\n" +
                    "  \"tips\":\"cassandra配置元数据，请误删除!!!!\"\n" +
                    "}";

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, json);
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/.cassandra_metadata/_doc/" + keyspace + "-" + table)
                    .method("PUT", body)
                    .addHeader("Content-Type", "application/json")
                    .build();
            Response response = client.newCall(request).execute();


            logger.info("创建元数据配置索引.cassandra_metadata返回：code:" + response.code() + "; 返回内容:" + response.body().string());
            if (ErrorEnum.SUCCESS.code != response.code()) {
                return DataRsp.builder()
                        .code(response.code())
                        .message(response.message())
                        .build();
            }

        } catch (Exception e) {
            logger.error("异常2", e);
        }
        return DataRsp.getError200();
    }


    private static boolean isExitsCassandraMetadata() {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/.cassandra_metadata")
                    .method("HEAD", null)
                    .build();
            Response response = client.newCall(request).execute();

            logger.info("判断cassnadra元数据索引是否存在:" + response.code());
            if (response.code() == 200) {
                return true;
            }
        } catch (Exception e) {
            logger.error("异常1", e);
        }
        return false;
    }

    public static boolean newGetSyncEs(String keyspace, String table) {

        Boolean aBoolean = CassandraUtil.syncTablesInfo.get(keyspace + "." + table);
        if (aBoolean == null) {
            String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
            try {

                OkHttpClient client = new OkHttpClient().newBuilder()
                        .build();
                MediaType mediaType = MediaType.parse("application/json");
                RequestBody body = RequestBody.create(mediaType, "{\n  \"query\": {\n    \"bool\": {\n      \"filter\": [\n        {\n          \"match\": {\n            \"keyspace.keyword\": \"" + keyspace + "\"\n          }\n        },\n        {\n          \"match\": {\n            \"table.keyword\": \"" + table + "\"\n          }\n        }\n      ]\n    }\n  }\n}");
                Request request = new Request.Builder().addHeader("Connection", "close")
                        .url(nodeUrl + "/.cassandra_metadata/_search")
                        .method("POST", body)
                        .addHeader("Content-Type", "application/json")
                        .build();
                Response response = client.newCall(request).execute();

                EsResDto esResDto = JSONObject.parseObject(response.body().string(), EsResDto.class);
                List<Hites> hits = esResDto.getHits().getHits();
                if (hits.size() != 0) {
                    return (boolean) hits.get(0).get_source().get("isSyncEs");
                }
                return false;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return aBoolean;
    }


    public static DataRsp deleteCassandraMetadata(String indexName) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {
            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("text/plain");
            RequestBody body = RequestBody.create(mediaType, "");
            Request request = new Request.Builder().addHeader("Connection", "close")
                    .url(nodeUrl + "/.cassandra_metadata/_doc/" + indexName)
                    .method("DELETE", body)
                    .build();
            Response response = client.newCall(request).execute();

            logger.info("删除元数据索引数据返回：code:" + response.code() + "; 返回内容:" + response.body().string());
            if (ErrorEnum.SUCCESS.code != response.code()) {
                return DataRsp.builder()
                        .code(response.code())
                        .message(response.message())
                        .build();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }

}
