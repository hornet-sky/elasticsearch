package my;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.ls.LSOutput;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RestClientTest {
    private RestHighLevelClient client = null;
    @Before
    public void init() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.0.51", 9200, "http"),
                        new HttpHost("192.168.0.52", 9200, "http"),
                        new HttpHost("192.168.0.53", 9200, "http")
                )
        );
    }
    // -- Analyze API --
    @Test
    public void testAnalyzer() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withGlobalAnalyzer("english",
                "Some text to analyze",
                "Some more text to analyze");
        AnalyzeResponse response = client.indices().analyze(request, RequestOptions.DEFAULT);
        for(AnalyzeResponse.AnalyzeToken token: response.getTokens()) {
            System.out.println("term -> " + token.getTerm() + ", type -> " + token.getType());
        }
    }

    @Test
    public void testAnalyzer2() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withGlobalAnalyzer("ik_smart",
                "中华人民共和国人民大会堂",
                "飞流直下三千尺，疑是银河落九天");
        AnalyzeResponse response = client.indices().analyze(request, RequestOptions.DEFAULT);
        for(AnalyzeResponse.AnalyzeToken token: response.getTokens()) {
            System.out.println("term -> " + token.getTerm() + ", type -> " + token.getType());
        }
        //result: 中华人民共和国、人民大会堂、飞流直下三千尺、疑是银河落九天
    }

    @Test
    public void testAnalyzer3() throws IOException {
        Map<String, Object> stopFilter = new HashMap<>();
        stopFilter.put("type", "stop");
        stopFilter.put("stopwords", new String[] {"to"});
        AnalyzeRequest request = AnalyzeRequest.buildCustomAnalyzer("standard")
                .addCharFilter("html_strip")
                .addTokenFilter("lowercase")
                .addTokenFilter(stopFilter)
                .build("<b>Some text to analyze</b>");
        AnalyzeResponse response = client.indices().analyze(request, RequestOptions.DEFAULT);
        for(AnalyzeResponse.AnalyzeToken token: response.getTokens()) {
            System.out.println("term -> " + token.getTerm() + ", type -> " + token.getType());
        }
        //result: some、text、analyze
    }

    @Test
    public void testNormalizer() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.buildCustomNormalizer()
                .addTokenFilter("lowercase")
                .build("<B>Bob</B>");
        AnalyzeResponse response = client.indices().analyze(request, RequestOptions.DEFAULT);
        for(AnalyzeResponse.AnalyzeToken token: response.getTokens()) {
            System.out.println("term -> " + token.getTerm() + ", type -> " + token.getType());
        }
        //result: <b>bob</b>
    }

    // -- Index API --
    @Test
    public void testCreateIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("java-api-demo-idx");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1))
            .mapping(createMapping("ik_max_word"));
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    public void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("java-api-demo-idx");
        request//.indicesOptions(IndicesOptions.lenientExpandOpen()) //lenientExpandOpen 表示“宽容模式”，比如删除不存在的索引不报错。
                .timeout(TimeValue.timeValueMinutes(2))
                .masterNodeTimeout("1m");
        try {
            AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
            System.out.println("isAcknowledged -> " + response.isAcknowledged());
        } catch (ElasticsearchException ex) {
            if (ex.status() == RestStatus.NOT_FOUND) {
                System.out.println("未找到该索引");
            }
        }
    }

    @Test
    public void testCreateIndex2() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest("java-api-demo-idx");
        request.settings(createSettings())
                .mapping(createMapping("my_ik_syno_max_word"));
        client.indices().createAsync(request, RequestOptions.DEFAULT, new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse response) {
                System.out.println("isAcknowledged -> " + response.isAcknowledged() + ", index -> " + response.index());
                // isAcknowledged -> true, index -> java-api-demo-idx
            }
            @Override
            public void onFailure(Exception e) { // 重复执行会报错
                System.err.println("error -> " + e.getMessage());
                // error -> Elasticsearch exception [type=resource_already_exists_exception, reason=index [java-api-demo-idx/j_IZhjl8Q8Ctzoxpk8HB8A] already exists]
            }
        });
        Thread.sleep(3000);
    }

    // Settings
    @Test
    public void testGetSettings() throws Exception {
        GetSettingsRequest request = new GetSettingsRequest().indices("java-api-demo-idx");
        // request.names("index.number_of_shards", "index.number_of_replicas"); // 没声明的参数不会被查出来
        request.includeDefaults(true);
        client.indices().getSettingsAsync(request, RequestOptions.DEFAULT, new ActionListener<GetSettingsResponse>() {
            @Override
            public void onResponse(GetSettingsResponse response) {
                String numberOfShardsStr = response.getSetting("java-api-demo-idx", "index.number_of_shards");
                System.out.println("numberOfShardsStr -> " + numberOfShardsStr);
                String numberOfReplicasStr = response.getSetting("java-api-demo-idx", "index.number_of_replicas");
                System.out.println("numberOfReplicasStr -> " + numberOfReplicasStr);

                Settings settings = response.getIndexToSettings().get("java-api-demo-idx");
                for(String key : settings.keySet()) {
                    if(key.contains("number_of_"))
                        System.out.println(key + " --> " + settings.getAsInt(key, null));
                     else
                         System.out.println(key + " --> " + settings.get(key));
                }

                settings = response.getIndexToDefaultSettings().get("java-api-demo-idx");
                for(String key : settings.keySet()) {
                    if(key.contains("number_of_"))
                        System.out.println(key + " ---> " + settings.getAsInt(key, null));
                    else
                        System.out.println(key + " ---> " + settings.get(key));
                }
            }
            @Override
            public void onFailure(Exception e) {
                System.err.println("err -> " + e.getMessage());
            }
        });
        Thread.sleep(3000);
    }

    // Mappings
    @Test
    public void testGetMappings() throws Exception {
        GetMappingsRequest request = new GetMappingsRequest().indices("java-api-demo-idx", "my_pinyin_idx");

        client.indices().getMappingAsync(request, RequestOptions.DEFAULT, new ActionListener<GetMappingsResponse>() {
            @Override
            public void onResponse(GetMappingsResponse response) {
                Map<String, MappingMetadata> allMappings = response.mappings();
                MappingMetadata mappingMetadata = allMappings.get("java-api-demo-idx");
                Map<String, Object> properties = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
                for(Map.Entry<String, Object> entry : properties.entrySet()) {
                    Map<String, Object> fieldDefs = (Map<String, Object>)entry.getValue();
                    System.out.println(entry.getKey());
                    for(Map.Entry<String, Object> fieldDefEntry :fieldDefs.entrySet()) {
                        System.out.println("\t" + fieldDefEntry.getKey() + " -> " + fieldDefEntry.getValue());
                    }
                }
                /*
                title
                    type -> text
                content
                    analyzer -> my_ik_syno_max_word
                    type -> text
                 */

                mappingMetadata = allMappings.get("my_pinyin_idx");
                properties = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
                for(Map.Entry<String, Object> entry : properties.entrySet()) {
                    Map<String, Object> fieldDefs = (Map<String, Object>)entry.getValue();
                    System.out.println(entry.getKey());
                    for(Map.Entry<String, Object> fieldDefEntry :fieldDefs.entrySet()) {
                        System.out.println("\t" + fieldDefEntry.getKey() + " ---> " + fieldDefEntry.getValue());
                    }
                }
                /*
                title
                    type ---> text
                content
                    analyzer ---> my_pinyin_analyzer
                    type ---> text
                 */
            }
            @Override
            public void onFailure(Exception e) {
                System.err.println("err -> " + e.getMessage());
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testUpdateMappings() throws Exception {
        PutMappingRequest request = new PutMappingRequest("java-api-demo-idx");
        // 只修改指定的条目，未指定的条目不受影响
        request.source("{\"properties\": {\"author\": {\"type\": \"keyword\"}}}", XContentType.JSON);
        client.indices().putMappingAsync(request, RequestOptions.DEFAULT, new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                System.out.println("isAcknowledged -> " + response.isAcknowledged());
            }
            @Override
            public void onFailure(Exception e) {
                System.err.println("err -> " + e.getMessage());
            }
        });
        Thread.sleep(3000);
    }

    // 文档操作
    @Test
    public void testAddDoc() throws Exception {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("author", "大黄");
        jsonMap.put("title", "大黄的幸福生活");
        jsonMap.put("content", "山重水复疑无路，柳暗花明又一村。");
        IndexRequest indexRequest = new IndexRequest("java-api-demo-idx")
                .id("2")
                //.opType(DocWriteRequest.OpType.CREATE) // 重复创建相同id的文档会报错。注释掉的话，既可以新增文档，也可以更新文档。
                .source(jsonMap);
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse response) {
                if(response.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("创建成功！");
                    System.out.println("id -> " + response.getId()); // id -> 1
                    System.out.println("index -> " + response.getIndex()); // index -> java-api-demo-idx
                } else if(response.getResult() == DocWriteResponse.Result.UPDATED) {
                    System.out.println("更新成功！");
                }
                ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
                System.out.println("total -> " + shardInfo.getTotal()
                        + ", successful -> " + shardInfo.getSuccessful()
                        + ", failed -> " + shardInfo.getFailed()); // total -> 2, successful -> 2, failed -> 0

                if(shardInfo.getFailed() > 0) {
                    for(ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        System.out.println("failure.reason -> " + failure.reason());
                    }
                }
            }
            @Override
            public void onFailure(Exception e) {
                System.err.println("err -> " + e.getMessage());
                if(e instanceof ElasticsearchException) {
                    ElasticsearchException ex = (ElasticsearchException) e;
                    if(ex.status() == RestStatus.CONFLICT) {
                        System.err.println("冲突了！");
                    }
                }
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testGet() throws Exception {
        GetRequest request = new GetRequest("java-api-demo-idx", "1");
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[] {"author", "title"};
        FetchSourceContext fsc = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fsc);
        // request.version(2); // 可能会导致版本冲突。因为每修改一次文档，版本都会自动加1。
        client.getAsync(request, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse documentFields) {
                if(!documentFields.isExists()) {
                    System.out.println("不存在id为" + documentFields.getId() + "的文档！");
                    return;
                }
                System.out.println("version -> " + documentFields.getVersion()); // version -> 2
                System.out.println("sourceAsString -> " + documentFields.getSourceAsString()); // sourceAsString -> {"content":"一个人的路注定艰难，谁说不是呢。"}
                for(Map.Entry<String, DocumentField> entry: documentFields.getFields().entrySet()) {
                    System.out.println(entry.getKey() + " -> " + entry.getValue());
                }
                for(Map.Entry<String, Object> entry: documentFields.getSourceAsMap().entrySet()) {
                    System.out.println(entry.getKey() + " ---> " + entry.getValue());
                }
                // content ---> 一个人的路注定艰难，谁说不是呢。
            }
            @Override
            public void onFailure(Exception e) {
                if(e instanceof ElasticsearchException) {
                    ElasticsearchException ex = (ElasticsearchException) e;
                    if(ex.status() == RestStatus.NOT_FOUND) {
                        System.err.println("未找到索引" + ex.getIndex());
                    } else if(ex.status() == RestStatus.CONFLICT) {
                        System.err.println("文档版本冲突了！");
                    } else {
                        System.err.println("其他问题！");
                    }
                } else {
                    System.err.println("报错了：" + e.getMessage());
                }
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testGetSource() throws Exception {
        GetSourceRequest request = new GetSourceRequest("java-api-demo-idx", "1");
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[] {"title"};
        FetchSourceContext fsc = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fsc);
        // request.version(2); // 可能会导致版本冲突。因为每修改一次文档，版本都会自动加1。
        client.getSourceAsync(request, RequestOptions.DEFAULT, new ActionListener<GetSourceResponse>() {
            @Override
            public void onResponse(GetSourceResponse response) {
                for(Map.Entry<String, Object> entry: response.getSource().entrySet()) {
                    System.out.println(entry.getKey() + " ---> " + entry.getValue());
                }
            }
            @Override
            public void onFailure(Exception e) {
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testMultiGet() throws Exception {
        MultiGetRequest request = new MultiGetRequest()
                .add(new MultiGetRequest.Item(
                        "java-api-demo-idx",
                        "1"))
                .add(new MultiGetRequest.Item(
                        "java-api-demo-idx",
                        "3"));
        client.mgetAsync(request, RequestOptions.DEFAULT, new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse multiGetItemResponses) {
                for(MultiGetItemResponse resp : multiGetItemResponses.getResponses()) {
                    System.out.println(resp.getResponse().getSourceAsString());
                }
            }
            @Override
            public void onFailure(Exception e) {
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testExists() throws Exception {
        GetRequest request = new GetRequest("java-api-demo-idx", "9");
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        boolean isExists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println("isExists -> " + isExists);
    }

    @Test
    public void testDelete() throws Exception {
        DeleteRequest request = new DeleteRequest("java-api-demo-idx", "2")
                // .routing("abc") // routing会影响操作分片的先后顺序，必须与存数据时的routing值一样才能找到目标文档，默认以id作为routing值。
                .timeout("2m")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL); // 等待es刷新索引后再结束请求。操作时延高，但数据一致性好，es资源消耗低。
        client.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                // 分片上执行成功与否
                ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
                System.out.println("total -> " + shardInfo.getTotal()
                        + ", successful -> " + shardInfo.getSuccessful()
                        + ", failed -> " + shardInfo.getFailed());
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    System.out.println("成功分片数小于总分片数！");
                } else {
                    System.out.println("每个分片都操作完成！");
                }
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure :
                            shardInfo.getFailures()) {
                        String reason = failure.reason();
                        System.out.println("分片上删除失败：" + reason);
                    }
                }
                // 操作结果
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    System.out.println("未找到文档：" + deleteResponse.getId());
                }
            }
            @Override
            public void onFailure(Exception e) {
                if(e instanceof ElasticsearchException) {
                    ElasticsearchException ex = (ElasticsearchException) e;
                    if(ex.status() == RestStatus.NOT_FOUND) {
                        System.err.println("未找到索引" + ex.getIndex());
                    } else if(ex.status() == RestStatus.CONFLICT) {
                        System.err.println("文档版本冲突了！");
                    } else {
                        System.err.println("其他问题！");
                    }
                } else {
                    System.err.println("报错了：" + e.getMessage());
                }
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testUpdate() throws Exception {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("author", "李四");
        jsonMap.put("title", "李四的自白");
        UpdateRequest request = new UpdateRequest("java-api-demo-idx", "3")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .doc(jsonMap); // 如果未找到文档会抛错，ex.status() == RestStatus.NOT_FOUND
        client.updateAsync(request, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                // 分片上执行成功与否
                ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
                System.out.println("total -> " + shardInfo.getTotal()
                        + ", successful -> " + shardInfo.getSuccessful()
                        + ", failed -> " + shardInfo.getFailed());
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    System.out.println("成功分片数小于总分片数！");
                } else {
                    System.out.println("每个分片都操作完成！");
                }
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure :
                            shardInfo.getFailures()) {
                        String reason = failure.reason();
                        System.out.println("分片上删除失败：" + reason);
                    }
                }
                // 执行结果
                if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("执行成功，已创建！");
                } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    System.out.println("执行成功，已更新！");
                } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                    System.out.println("执行成功，已删除！");
                } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                    System.out.println("执行成功，无操作！");
                }
            }
            @Override
            public void onFailure(Exception e) {
                if(e instanceof ElasticsearchException) {
                    ElasticsearchException ex = (ElasticsearchException) e;
                    if(ex.status() == RestStatus.NOT_FOUND) {
                        System.err.println("未找到索引" + ex.getIndex());
                    } else if(ex.status() == RestStatus.CONFLICT) {
                        System.err.println("文档版本冲突了！");
                    } else {
                        System.err.println("其他问题！");
                    }
                } else {
                    System.err.println("报错了：" + e.getMessage());
                }
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testUpsert() throws Exception {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("author", "lisi");
        jsonMap.put("title", "李四的独白");
        jsonMap.put("content", "你好");
        UpdateRequest request = new UpdateRequest("java-api-demo-idx", "2")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .upsert(jsonMap)
                .doc(jsonMap);
        client.updateAsync(request, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                // 分片上执行成功与否
                ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
                System.out.println("total -> " + shardInfo.getTotal()
                        + ", successful -> " + shardInfo.getSuccessful()
                        + ", failed -> " + shardInfo.getFailed());
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    System.out.println("成功分片数小于总分片数！");
                } else {
                    System.out.println("每个分片都操作完成！");
                }
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure :
                            shardInfo.getFailures()) {
                        String reason = failure.reason();
                        System.out.println("分片上删除失败：" + reason);
                    }
                }
                // 执行结果
                if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("执行成功，已创建！");
                } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    System.out.println("执行成功，已更新！");
                } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                    System.out.println("执行成功，已删除！");
                } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                    System.out.println("执行成功，无操作！");
                }
            }
            @Override
            public void onFailure(Exception e) {
                if(e instanceof ElasticsearchException) {
                    ElasticsearchException ex = (ElasticsearchException) e;
                    if(ex.status() == RestStatus.NOT_FOUND) {
                        System.err.println("未找到索引" + ex.getIndex());
                    } else if(ex.status() == RestStatus.CONFLICT) {
                        System.err.println("文档版本冲突了！");
                    } else {
                        System.err.println("其他问题！");
                    }
                } else {
                    System.err.println("报错了：" + e.getMessage());
                }
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testUpdateByQuery() throws Exception {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("author", "李四");
        jsonMap.put("title", "李四的感慨");
        jsonMap.put("content", "hello world！");
        String code = "ctx._source.author = params.author; ctx._source.title = params.title; ctx._source.content = params.content;";
        UpdateByQueryRequest request = new UpdateByQueryRequest("java-api-demo-idx")
                .setQuery(new TermQueryBuilder("author", "lisi"))
                .setScript(new Script(ScriptType.INLINE, "painless", code, jsonMap));
        client.updateByQueryAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                System.out.println("total -> " + bulkByScrollResponse.getTotal()
                        + ", created -> " + bulkByScrollResponse.getCreated()
                        + ", updated -> " + bulkByScrollResponse.getUpdated()
                        + ", deleted -> " + bulkByScrollResponse.getDeleted());
            }
            @Override
            public void onFailure(Exception e) {
                System.err.println("err -> " + e.getMessage());
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testDeleteByQuery() throws Exception {
        DeleteByQueryRequest request = new DeleteByQueryRequest("java-api-demo-idx")
                .setQuery(new TermQueryBuilder("author", "李四"));
                //.setQuery(QueryBuilders.matchAllQuery()); // 删除全部
        client.deleteByQueryAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                System.out.println("total -> " + bulkByScrollResponse.getTotal()
                        + ", created -> " + bulkByScrollResponse.getCreated()
                        + ", updated -> " + bulkByScrollResponse.getUpdated()
                        + ", deleted -> " + bulkByScrollResponse.getDeleted());
            }
            @Override
            public void onFailure(Exception e) {
                System.err.println("err -> " + e.getMessage());
            }
        });
        Thread.sleep(3000);
    }

    @Test
    public void testBulk() throws Exception {
        BulkRequest request = new BulkRequest()
                .add(
                        new IndexRequest("java-api-demo-idx").id("3")
                                .source(XContentType.JSON, "author", "王五", "salary", 56000.0, "title", "中华人民共和国台湾事务办公室", "content", "中华人民共和国台湾事务办公室"),
                        new IndexRequest("java-api-demo-idx").id("4")
                                .source(XContentType.JSON, "author", " 赵六", "salary", 23000.0, "title", "中华人民共和国人民大会堂", "content", "中华人民共和国人民大会堂"),
                        new IndexRequest("java-api-demo-idx").id("5")
                                .source(XContentType.JSON, "author", "王五", "salary", 56000.0, "title", "中国人民解放军", "content", "中国人民解放军"),
                        new IndexRequest("java-api-demo-idx").id("6")
                                .source(XContentType.JSON, "author", "小明", "salary", 31000.0, "title", "国务院台湾事务办公室", "content", "国务院台湾事务办公室"),
                        new IndexRequest("java-api-demo-idx").id("7")
                                .source(XContentType.JSON, "author", "李四", "salary", 38000.0, "title", "中字头股票", "content", "中字头股票"),

                        new DeleteRequest("java-api-demo-idx", "1"), // 删除

                        new UpdateRequest("java-api-demo-idx", "2") // 更新
                                .doc(XContentType.JSON, "author", "大黄", "salary", 66000.0, "title", "中概股")
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .timeout("2m");

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("bulkResponse.hasFailures -> " + bulkResponse.hasFailures()); // false
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            if(bulkItemResponse.isFailed()) {
                System.out.println("bulkItemResponse.getFailureMessage -> " + bulkItemResponse.getFailureMessage());
            }
            switch (bulkItemResponse.getOpType()) {
                case INDEX:
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                    System.out.println("create: index = " + indexResponse.getIndex() + ", result = " + indexResponse.getResult()); // create: index = java-api-demo-idx, result = CREATED
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                    System.out.println("update: index = " + updateResponse.getIndex() + ", result = " + updateResponse.getResult()); // update: index = java-api-demo-idx, result = UPDATED
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                    System.out.println("delete: index = " + deleteResponse.getIndex() + ", result = " + deleteResponse.getResult()); // delete: index = java-api-demo-idx, result = DELETED
            }
        }
        //http://192.168.0.51:9200/java-api-demo-idx/_doc/3/_termvectors?pretty&fields=content
    }

    @Test
    public void testSearch() throws Exception {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery())
                .from(0)
                .size(10)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));
        SearchRequest request = new SearchRequest("java-api-demo-idx")
                .source(sourceBuilder);

        SearchResponse resp = client.search(request, RequestOptions.DEFAULT);
        System.out.println("totalShards -> " + resp.getTotalShards());
        System.out.println("successfulShards -> " + resp.getSuccessfulShards());
        SearchHits hits = resp.getHits();
        System.out.println("totalHits -> " + hits.getTotalHits().value);
        System.out.println("maxScore -> " + hits.getMaxScore());
        for(SearchHit hit : hits.getHits()) {
            System.out.println("id = " + hit.getId() + ", score = " + hit.getScore() + ", sourceAsString = " + hit.getSourceAsString());
        }
    }

    @Test
    public void testSearch2() throws Exception {
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("content", "中国国台办"); // 分词，然后逐词匹配
        // QueryBuilder matchQueryBuilder = QueryBuilders.termQuery("content", "中国国台办"); // 不分词，使用"中国国台办"整体查询
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(matchQueryBuilder)
                .from(0)
                .size(10)
                .timeout(new TimeValue(60, TimeUnit.SECONDS))
                .sort(new ScoreSortBuilder().order(SortOrder.DESC)) // 不写默认也是按分数降序排列
                //.sort(new FieldSortBuilder("_id").order(SortOrder.DESC))
                //.fetchSource(includeFields, excludeFields); // 指定搜索结果包含的字段、排除的字段
                ;
        SearchRequest request = new SearchRequest("java-api-demo-idx")
                .source(sourceBuilder);

        SearchResponse resp = client.search(request, RequestOptions.DEFAULT);
        System.out.println("totalShards -> " + resp.getTotalShards());
        System.out.println("successfulShards -> " + resp.getSuccessfulShards());
        SearchHits hits = resp.getHits();
        System.out.println("totalHits -> " + hits.getTotalHits().value);
        System.out.println("maxScore -> " + hits.getMaxScore());
        for(SearchHit hit : hits.getHits()) {
            System.out.println("id = " + hit.getId() + ", score = " + hit.getScore() + ", sourceAsString = " + hit.getSourceAsString());
        }
    }

    @Test
    public void testSearch3() throws Exception {
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("byAuthor")
                .field("author"); // author 本身就是keyword类型，因此不需要"author.keyword"
        aggregation.subAggregation(AggregationBuilders.count("countDoc") // 统计每人的文档数
                .field("_none_"));
        aggregation.subAggregation(AggregationBuilders.avg("average_salary") // 还有sum、max、min等聚合方法
                .field("salary")); // 计算平均薪水
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("content", "中国国台办"))
                .aggregation(aggregation); // 从查询到的结果中做统计
        SearchRequest request = new SearchRequest("java-api-demo-idx")
                .source(sourceBuilder);

        SearchResponse resp = client.search(request, RequestOptions.DEFAULT);
        System.out.println("totalShards -> " + resp.getTotalShards()); // 3
        System.out.println("successfulShards -> " + resp.getSuccessfulShards()); // 3
        SearchHits hits = resp.getHits();
        System.out.println("totalHits -> " + hits.getTotalHits().value); // 4
        System.out.println("maxScore -> " + hits.getMaxScore()); // 9.63375
        for(SearchHit hit : hits.getHits()) { // 正常显示_source内容，如果用不着_source内容，可以通过设置去掉
            System.out.println("id = " + hit.getId() + ", score = " + hit.getScore() + ", sourceAsString = " + hit.getSourceAsString());
        }

        // Aggregations
        Aggregations aggregations = resp.getAggregations();
        Terms byAuthorAggregation = aggregations.get("byAuthor");
        Terms.Bucket bucket = byAuthorAggregation.getBucketByKey("王五");
        System.out.println("docCount -> " + (bucket != null ? bucket.getDocCount() : "无")); // 2
        Avg averageSalary = bucket.getAggregations().get("average_salary");
        System.out.println("avgSalary -> " + averageSalary.getValue()); // 56000.0
    }

    private XContentBuilder createSettings() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("number_of_shards", 3);
            builder.field("number_of_replicas", 1);
            builder.startObject("analysis");
            {
                builder.startObject("filter");
                {
                    builder.startObject("my_remote_synonym");
                    {
                        builder.field("type", "dynamic_synonym");
                        builder.field("synonyms_path", "http://192.168.0.56/syns/mysynonym.txt");
                        builder.field("interval", 30);
                    }
                    builder.endObject();
                    builder.startObject("my_local_synonym");
                    {
                        builder.field("type", "dynamic_synonym");
                        builder.field("synonyms_path", "mylocalsynonym.txt");
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("analyzer");
                {
                    builder.startObject("my_ik_syno_max_word");
                    {
                        builder.field("tokenizer", "ik_max_word");
                        builder.array("filter", new String[] {"my_remote_synonym", "my_local_synonym"});
                    }
                    builder.endObject();
                    builder.startObject("my_ik_syno_smart");
                    {
                        builder.field("tokenizer", "ik_smart");
                        builder.array("filter", new String[] {"my_remote_synonym", "my_local_synonym"});
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    private Map<String, Object> createMapping(String analyzer) {
        Map<String, Object> title = new HashMap<>();
        title.put("type", "text");

        Map<String, Object> content = new HashMap<>();
        content.put("type", "text");
        content.put("analyzer", analyzer);

        Map<String, Object> properties = new HashMap<>();
        properties.put("title", title);
        properties.put("content", content);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        return mapping;
    }
}
