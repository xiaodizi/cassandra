package org.apache.cassandra.audit.es;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.cassandra.audit.es.common.ErrorEnum;
import org.apache.cassandra.audit.es.dto.EsClusterDto;
import org.apache.cassandra.audit.es.dto.EsResDto;
import org.apache.cassandra.audit.es.dto.Hites;
import org.apache.cassandra.audit.es.res.DataRsp;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpUtil {

    private static final Logger logger = LoggerFactory.getLogger(HttpUtil.class);

    static {
        Unirest.setTimeouts(1000, 2000);
    }

    public static DataRsp getClusterHealth() {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        EsClusterDto esClusterDto = null;
        try {
            HttpResponse<String> response = Unirest.get(nodeUrl + "/_cluster/health")
                    .asString();
            logger.info("获取集群健康状态，返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
            if (response.getStatus() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }
            esClusterDto = JSONObject.parseObject(response.getBody(), EsClusterDto.class);

        } catch (UnirestException e) {
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
            HttpResponse<String> response = Unirest.put(nodeUrl + "/" + indexName)
                    .header("Content-Type", "application/json")
                    .body("{\n  \"settings\":{\n    \"number_of_shards\":" + numSharedNodes + ",\n    \"number_of_replicas\":" + (numSharedNodes - 1) + "\n  }\n}")
                    .asString();
            logger.info("创建索引返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
            if (response.getStatus() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }

    /**
     * 创建索引
     *
     */
    public static DataRsp createIndex(String indexName, String json, String id) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {
            HttpResponse<String> response = Unirest.put(nodeUrl + "/" + indexName + "/_doc/" + id)
                    .header("Content-Type", "application/json")
                    .body(json)
                    .asString();


            logger.info("Insert 数据返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());

            if (response.getStatus() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        return DataRsp.getError200();
    }


    public static DataRsp bulkIndex(String indexName, Map<String, Object> maps) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {
            String bulkApiJson = EsUtil.getBulkCreateApiJson(maps);
            System.out.println("bulkApiJson:"+bulkApiJson);
            HttpResponse<String> response = Unirest.post(nodeUrl + "/" + indexName + "/_bulk")
                    .header("Content-Type", "application/x-ndjson")
                    .body(bulkApiJson)
                    .asString();
            logger.info("Bulk 数据返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }


    public static DataRsp bulkUpdate(String indexName, Map<String, Object> maps, String docId) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {
            String bulkApiJson = EsUtil.getBulkUpdateApiJson(maps, docId);
            HttpResponse<String> response = Unirest.post(nodeUrl + "/" + indexName + "/_bulk")
                    .header("Content-Type", "application/x-ndjson")
                    .body(bulkApiJson)
                    .asString();
            logger.info("Bulk 数据返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }

    /**
     * 查询方法,match  keyword
     * 多条件 使用bool 查询
     */
    public static DataRsp<Object> getSearch(String indexName, Map<String, Object> maps) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        List<Hites> hitesList = new ArrayList<>();
        try {

            String requestJson = EsUtil.getDslQueryJson(maps);
            logger.info("查询 DSL：" + requestJson);
            HttpResponse<String> response = Unirest.post(nodeUrl + "/" + indexName + "/_search")
                    .header("Content-Type", "application/json")
                    .body(requestJson)
                    .asString();
            if (response.getStatus() != ErrorEnum.SUCCESS.code) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .data(hitesList)
                        .build();
            }
            logger.info("结果1：" + response.getBody());
            EsResDto esResDto = JSONObject.parseObject(response.getBody(), EsResDto.class);
            logger.info("结果2：" + JSON.toJSONString(esResDto));
            hitesList = esResDto.getHits().getHits();

        } catch (UnirestException e) {
            e.printStackTrace();
        }

        return DataRsp.builder().code(ErrorEnum.SUCCESS.code)
                .message(ErrorEnum.SUCCESS.message)
                .data(hitesList)
                .build();
    }

    /**
     * 获取索引是否存在
     * 通过ID 查询
     *
     */
    public static DataRsp<Object> getIndex(String indexName, String id) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {
            HttpResponse<String> response = Unirest.get(nodeUrl + "/" + indexName + "/_doc/" + id)
                    .header("Content-Type", "application/json")
                    .asString();

            if (ErrorEnum.SUCCESS.code != response.getStatus()) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }

    /**
     * 删除数据
     *
     */
    public static DataRsp deleteData(String indexName, Map maps) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {
            String requestJson = EsUtil.getDslQueryJson(maps);
            logger.info("删除 DSL：" + requestJson);
            HttpResponse<String> response = Unirest.post(nodeUrl + "/" + indexName + "/_delete_by_query?slices=auto&conflicts=proceed&wait_for_completion=false")
                    .header("Content-Type", "application/json")
                    .body(requestJson)
                    .asString();
            logger.info("删除返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
            if (ErrorEnum.SUCCESS.code != response.getStatus()) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }


    /**
     * 删除索引
     *
     */
    public static DataRsp dropIndex(String indexName) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {

            HttpResponse<String> response = Unirest.delete(nodeUrl + "/" + indexName)
                    .asString();

            logger.info("删除索引返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
            if (ErrorEnum.SUCCESS.code != response.getStatus()) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }

        } catch (Exception e) {
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
            HttpResponse<String> response = Unirest.put(nodeUrl + "/.cassandra_metadata/_doc/" + keyspace + "-" + table)
                    .header("Content-Type", "application/json")
                    .body(json)
                    .asString();

            logger.info("创建元数据配置索引.cassandra_metadata返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
            if (ErrorEnum.SUCCESS.code != response.getStatus()) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }

        } catch (Exception e) {
            logger.error("异常2",e);
        }
        return DataRsp.getError200();
    }


    private static boolean isExitsCassandraMetadata() {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {
            HttpResponse<String> response = Unirest.head(nodeUrl + "/.cassandra_metadata")
                    .asString();
            logger.info("判断cassnadra元数据索引是否存在:"+response.getStatus());
            if (response.getStatus() == 200) {
                return true;
            }
        } catch (UnirestException e) {
            logger.error("异常1",e);
        }
        return false;
    }

    public static boolean newGetSyncEs(String keyspace, String table) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";

        try {
            HttpResponse<String> response = Unirest.post(nodeUrl+"/.cassandra_metadata/_search")
                    .header("Content-Type", "application/json")
                    .body("{\n  \"query\": {\n    \"bool\": {\n      \"filter\": [\n        {\n          \"match\": {\n            \"keyspace.keyword\": \""+keyspace+"\"\n          }\n        },\n        {\n          \"match\": {\n            \"table.keyword\": \""+table+"\"\n          }\n        }\n      ]\n    }\n  }\n}")
                    .asString();

            EsResDto esResDto = JSONObject.parseObject(response.getBody(), EsResDto.class);
            List<Hites> hits = esResDto.getHits().getHits();
            if (hits.size() != 0){
                return (boolean) hits.get(0).get_source().get("isSyncEs");
            }
        } catch (UnirestException e) {
            e.printStackTrace();
        }
        return true;
    }



    public static DataRsp deleteCassandraMetadata(String indexName) {
        String nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        try {

            HttpResponse<String> response = Unirest.delete(nodeUrl + "/.cassandra_metadata/_doc/"+indexName)
                    .header("Content-Type", "application/json")
                    .asString();
            logger.info("删除元数据索引数据返回：code:" + response.getStatus() + "; 返回内容:" + response.getBody());
            if (ErrorEnum.SUCCESS.code != response.getStatus()) {
                return DataRsp.builder()
                        .code(response.getStatus())
                        .message(response.getStatusText())
                        .build();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return DataRsp.getError200();
    }

}
