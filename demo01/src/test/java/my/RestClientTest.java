package my;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.ls.LSOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

    @Test
    public void testDeleteMappings() throws Exception {

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
