package org.apache.cassandra.audit.es;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.cassandra.audit.AuditLogEntry;
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

    public static DataRsp getClusterHealth(String url) {
        String nodeUrl = getRandomNode(url);
        logger.info("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        EsClusterDto esClusterDto = null;
        Unirest.setTimeouts(0, 0);
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


    public static DataRsp newCreateIndex(String url, String indexName) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }

        DataRsp clusterHealth = getClusterHealth(url);
        int numSharedNodes = Integer.parseInt(clusterHealth.getData().toString());

        Unirest.setTimeouts(0, 0);
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
     * @param url
     * @param indexName
     * @param json
     * @param id
     * @return
     */
    public static DataRsp createIndex(String url, String indexName, String json, String id) {
        String nodeUrl = getRandomNode(url);
        logger.info("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        Unirest.setTimeouts(0, 0);
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


    public static DataRsp bulkIndex(String url, String indexName, Map<String, Object> maps) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        Unirest.setTimeouts(0, 0);
        try {
            String bulkApiJson = EsUtil.getBulkCreateApiJson(maps);
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


    public static DataRsp bulkUpdate(String url, String indexName, Map<String, Object> maps, String docId) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        Unirest.setTimeouts(0, 0);
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
     *
     * @param url
     * @param indexName
     * @return
     */
    public static DataRsp<Object> getSearch(String url, String indexName, Map<String, Object> maps) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        List<Hites> hitesList = new ArrayList<>();
        Unirest.setTimeouts(0, 0);
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
     * @param url
     * @param indexName
     * @param id
     * @return 非200都是不存在
     */
    public static DataRsp<Object> getIndex(String url, String indexName, String id) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        Unirest.setTimeouts(0, 0);
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
     * @param url
     * @param indexName
     * @param maps
     * @return
     */
    public static DataRsp deleteData(String url, String indexName, Map maps) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        Unirest.setTimeouts(0, 0);
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


    public static String getRandomNode(String esNodeList) {
        if (StringUtils.isBlank(esNodeList)) {
            logger.info("LEI TEST WARN :es_node_list 配置为空,");
            return "";
        }
        String[] nodeList = esNodeList.split(",");
        int index = (int) (Math.random() * nodeList.length);
        return nodeList[index];
    }

    /**
     * 删除索引
     *
     * @param url
     * @param indexName
     * @return
     */
    public static DataRsp dropIndex(String url, String indexName) {

        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        Unirest.setTimeouts(0, 0);
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


    public static DataRsp createCassandraMetadata(String url, String keyspace, String table, boolean isSyncEs) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }

        if (!isExitsCassandraMetadata(url)) {
            newCreateIndex(url, ".cassandra_metadata");
        }

        Unirest.setTimeouts(0, 0);
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


    private static boolean isExitsCassandraMetadata(String url) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        Unirest.setTimeouts(0, 0);
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

    public static boolean newGetSyncEs(String url, String keyspace, String table) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }

        Unirest.setTimeouts(0, 0);
        try {
            Unirest.setTimeouts(0, 0);
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



    public static DataRsp deleteCassandraMetadata(String url, String indexName) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            nodeUrl = "http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":9200";
        }
        Unirest.setTimeouts(0, 0);
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
