package cn.itcast.hotel;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONUtil;
import cn.itcast.hotel.pojo.HotelDoc;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class MatchAllTest {

    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://10.38.128.128:9200")
        ));
    }

    //测试发送所有
    @Test
    void testMatchAll() throws IOException {
        //1.准备请求对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        request.source().query(QueryBuilders.matchAllQuery());
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        extracted(response);

    }


    //测试根据条件查询
    @Test
    void testMatch() throws IOException {
        //1.准备请求对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析响应
        extracted(response);

    }

    //通过bool复合查询
    @Test
    void testMatchBool() throws IOException {
        //1.准备请求对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        // 2.1准备BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //2.2添加term
        boolQuery.must(QueryBuilders.termQuery("city", "上海"));
        //2.3添加range
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(1000));
        //2.4将boolQuery传入
        request.source().query(boolQuery);
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析响应
        extracted(response);

    }

    //对搜索结果排序和分页
    @Test
    void testMatchSort() throws IOException {
        //当前页 每页大小 计算页码关系
        int page = 2, size = 5;
        //构建请求
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        //分页
        request.source().from((page - 1) * size).size(size);
        //价格排序
        request.source().sort("price", SortOrder.DESC);
        //发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //处理请求
        extracted(response);
    }

    //测试内容高亮
    @Test
    void testMatchHighLight() throws IOException {
        //构建请求
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        //高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        //发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //处理请求
        extractedByHighLight(response);
    }

    private void extractedByHighLight(SearchResponse response) {
        SearchHits responseHits = response.getHits();

        SearchHit[] hits = responseHits.getHits();

        for (SearchHit hit : hits) {
            //将source转化为hotelDoc
            String source = hit.getSourceAsString();
            HotelDoc hotelDoc = JSONUtil.toBean(source, HotelDoc.class);

            Map<String, HighlightField> highlightMaps = hit.getHighlightFields();
            if (CollectionUtil.isNotEmpty(highlightMaps)){
                HighlightField highlightField = highlightMaps.get("name");
                if (highlightField != null) {
                    //取出高亮结果中的第一个
                    String name = highlightField.getFragments()[0].toString();
                    hotelDoc.setName(name);
                }
            }
            //打印结果
            System.out.println(hotelDoc);
        }

    }


    private static void extracted(SearchResponse response) {
        //4.解析结果
        SearchHits searchHits = response.getHits();

        //5.查询总条数
        long total = searchHits.getTotalHits().value;

        System.err.println("总共查询到" + total + "条数据");

        //6.查询结果数组
        SearchHit[] hits = searchHits.getHits();

        //7.对数组循环
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            //打印
            System.out.println(json);
        }
    }

    @AfterEach
    void close() throws IOException {
        this.client.close();
    }


}
