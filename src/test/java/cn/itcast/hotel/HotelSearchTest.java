package cn.itcast.hotel;

import cn.itcast.hotel.service.IHotelService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class HotelSearchTest {

    @Resource
    private RestHighLevelClient client;

    @Resource
    private IHotelService hotelService;

    @Test
    void testAggregation() throws IOException {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        //2.1设置size
        request.source().size(0);
        //2.2聚合
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(10)
        );
        //3.发出请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        Aggregations aggregations = response.getAggregations();
        //4.1根据聚合名称获取聚合结果
        Terms brandTerms = aggregations.get("brandAgg");
        //4.2获取buckets
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        //4.3遍历buckets
        for (Terms.Bucket bucket : buckets) {
            //4.4获取key
            String key = bucket.getKey().toString();
            System.out.println(key);
            //4.5获取bucket中的文档数量
            long docCount = bucket.getDocCount();
            System.out.println(docCount);
        }
    }

    @Test
    void testSuggest() throws IOException {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions",
                SuggestBuilders.completionSuggestion("suggestion")
                        .prefix("h")
                        .skipDuplicates(true)
                        .size(10)
        ));
        //3.发起请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        System.out.println(response);
        //4.1获取suggest
        Suggest suggest = response.getSuggest();
        //4.2获取suggest中的suggestion
        CompletionSuggestion suggestion = suggest.getSuggestion("suggestions");
        //4.4对suggestion循环
        for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
            //4.5循环项中的text就为补全点的词条
            String text = option.getText().string();
            System.out.println(text);
        }
    }


}
