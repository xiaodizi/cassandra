package org.apache.cassandra.audit.es;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import okhttp3.*;
import org.apache.cassandra.audit.es.common.ErrorEnum;
import org.apache.cassandra.audit.es.dto.EsClusterDto;
import org.apache.cassandra.audit.es.dto.EsResDto;
import org.apache.cassandra.audit.es.dto.Hites;
import org.apache.cassandra.audit.es.res.DataRsp;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpUtil {

    public static DataRsp getClusterHealth(String url){
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        EsClusterDto esClusterDto=null;
        try {

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            Request request = new Request.Builder().addHeader("Connection","close")
                    .url(nodeUrl+"/_cluster/health")
                    .build();
            Response response = client.newCall(request).execute();

            String bodyMessage=response.body().string();

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


    public static DataRsp newCreateIndex(String url,String indexName){
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }

        DataRsp clusterHealth = getClusterHealth(url);
        int numSharedNodes = Integer.parseInt(clusterHealth.getData().toString());

        try {
            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, "{\n  \"settings\":{\n    \"number_of_shards\":"+numSharedNodes+",\n    \"number_of_replicas\":"+(numSharedNodes-1)+"\n  }\n}");
            Request request = new Request.Builder().addHeader("Connection","close")
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
        }catch (Exception e){
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }


    public static DataRsp createIndex(String url, String indexName, String json, String id) {
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        try {

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, json);
            Request request = new Request.Builder().addHeader("Connection","close")
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


    public static DataRsp bulkIndex(String url, String indexName, Map<String, Object> maps, String keyValue) {
        String nodeUrl = "http://127.0.0.1:9200";
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }

        try {
            String bulkApiJson = EsUtil.getBulkCreateApiJson(maps,keyValue);
            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, bulkApiJson);
            Request request = new Request.Builder().addHeader("Connection","close")
                    .url(nodeUrl+"/"+indexName+"/_bulk")
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


    public static DataRsp bulkUpdate(String url,String indexName,Map<String,Object> maps,String docId){
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        try {
            String bulkApiJson = EsUtil.getBulkUpdateApiJson(maps,docId);

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, bulkApiJson);
            Request request = new Request.Builder().addHeader("Connection","close")
                    .url(nodeUrl+"/"+indexName+"/_bulk")
                    .method("POST", body)
                    .addHeader("Content-Type", "application/json")
                    .build();
            Response response = client.newCall(request).execute();

            System.out.println("Bulk 数据返回：code:" + response.code()+ "; 返回内容:" + response.body().string());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return DataRsp.getError200();
    }


    public static DataRsp<Object> getSearch(String url, String indexName, Map<String, Object> maps) {
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        List<Hites> hitesList = new ArrayList<>();
        try {

            String requestJson = EsUtil.getDslQueryJson(maps);
            System.out.println("查询 DSL：" + requestJson);
            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, requestJson);
            Request request = new Request.Builder().addHeader("Connection","close")
                    .url(nodeUrl + "/" + indexName + "/_search")
                    .method("POST", body)
                    .addHeader("Content-Type", "application/json")
                    .build();
            Response response = client.newCall(request).execute();

            String respBody=response.body().string();

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


    public static DataRsp<Object> getIndex(String url, String indexName, String id) {
        String nodeUrl = getRandomNode(url);
        System.out.println("LEI TEST INFO: 节点地址:" + nodeUrl);
        if (StringUtils.isBlank(nodeUrl)) {
            // es_node_list 配置为空 返回 406
            return DataRsp.getError406();
        }
        try {

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("text/plain");
            RequestBody body = RequestBody.create(mediaType, "");
            Request request = new Request.Builder().addHeader("Connection","close")
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


    public static DataRsp deleteData(String url, String indexName, Map maps) {
        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            return DataRsp.getError406();
        }
        try {
            String requestJson = EsUtil.getDslQueryJson(maps);
            System.out.println("删除 DSL：" + requestJson);

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, requestJson);
            Request request = new Request.Builder().addHeader("Connection","close")
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


    public static String getRandomNode(String esNodeList) {
        if (StringUtils.isBlank(esNodeList)) {
            System.out.println("LEI TEST WARN :es_node_list 配置为空,");
            return "";
        }
        String[] nodeList = esNodeList.split(",");
        int index = (int) (Math.random() * nodeList.length);
        return nodeList[index];
    }


    public static DataRsp dropIndex(String url, String indexName) {

        String nodeUrl = getRandomNode(url);
        if (StringUtils.isBlank(nodeUrl)) {
            return DataRsp.getError406();
        }
        try {

            OkHttpClient client = new OkHttpClient().newBuilder()
                    .build();
            MediaType mediaType = MediaType.parse("text/plain");
            RequestBody body = RequestBody.create(mediaType, "");
            Request request = new Request.Builder().addHeader("Connection","close")
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

}
