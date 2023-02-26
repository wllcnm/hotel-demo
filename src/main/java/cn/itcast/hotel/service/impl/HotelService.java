package cn.itcast.hotel.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Resource
    private RestHighLevelClient client;

    @Override
    public PageResult getHotels(RequestParams requestParams) {
        Integer page = requestParams.getPage();
        Integer size = requestParams.getSize();
        try {
            //1.准备request
            SearchRequest request = new SearchRequest("hotel");
            //2.准备dsl语句
            QueryBuilder(requestParams, request);
            //3.分页
            request.source().from((page - 1) * size).size(size);
            //价格排序
            String sortBy = requestParams.getSortBy();
            if (!"default".equals(sortBy)) {
                request.source().sort(sortBy, SortOrder.ASC);
            }
            //距离排序
            String location = requestParams.getLocation();
            if (location != null) {
                request.source().sort(SortBuilders
                        .geoDistanceSort("location", new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS)
                );
            }

            //4.发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //5.解析响应
            return ResponseHandler(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static void QueryBuilder(RequestParams requestParams, SearchRequest request) {
        String key = requestParams.getKey();
        //2.封装dsl 封住关键字查询 和按价格排序
        //2.1.query 需要用到复合查询,所以用到boolQuery
        //构建boolQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //关键字搜索
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        //条件过滤
        //城市
        if (StrUtil.isNotEmpty(requestParams.getCity())) {
            boolQuery.must(QueryBuilders.termQuery("city", requestParams.getCity()));
        }
        //品牌
        if (StrUtil.isNotEmpty(requestParams.getBrand())) {
            boolQuery.must(QueryBuilders.termQuery("brand", requestParams.getBrand()));
        }
        //星级
        if (StrUtil.isNotEmpty(requestParams.getStarName())) {
            boolQuery.must(QueryBuilders.termQuery("starName", requestParams.getStarName()));
        }
        //价格范围
        if (requestParams.getMinPrice() != null || requestParams.getMaxPrice() != null) {
            boolQuery.must(QueryBuilders.rangeQuery("price").lte(requestParams.getMaxPrice()).gte(requestParams.getMinPrice()));
        }

        //2.构建算分控制
        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(
                boolQuery, //将boolQuery放到算分控制中
                //Function Score的数组
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        //其中一个function score元素
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                //过滤条件
                                QueryBuilders.termQuery("isAD", true),
                                //算分函数
                                ScoreFunctionBuilders.weightFactorFunction(10)
                        )
                }
        );
        //将functionScoreQueryBuilder放到source
        request.source().query(functionScoreQueryBuilder);
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        try {
            HashMap<String, List<String>> bucketMap = new HashMap<>();
            //1.准备request
            SearchRequest request = new SearchRequest("hotel");
            //2.准备dsl
            //2.0设置query
            QueryBuilder(params, request);
            //2.1设置size
            request.source().size(0);
            //2.2聚合
            buildAggregation(request);
            //3.发出请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4.解析结果
            Aggregations aggregations = response.getAggregations();
            //5.获取list
            //根据品牌名称获取品牌结果
            ArrayList<String> brandList = getAggByName(aggregations, "brandAgg");
            //根据城市名称获取品牌结果
            ArrayList<String> cityList = getAggByName(aggregations, "cityAgg");
            //根据星级名称获取品牌结果
            ArrayList<String> starList = getAggByName(aggregations, "starAgg");
            //5.1将list加入到map中
            bucketMap.put("brand", brandList);
            bucketMap.put("city", cityList);
            bucketMap.put("starName", starList);
            //返回map
            return bucketMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestions(String key) {
        try {
            ArrayList<String> prefixList = new ArrayList<>();
            //1.准备request
            SearchRequest request = new SearchRequest("hotel");
            //2.准备dsl
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(key)
                            .skipDuplicates(true)
                            .size(10)
            ));
            //3.发起请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4.解析结果
            //4.1获取suggest
            Suggest suggest = response.getSuggest();
            //4.2获取suggest中的suggestion
            CompletionSuggestion suggestion = suggest.getSuggestion("suggestions");
            //4.4对suggestion循环
            for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
                //4.5循环项中的text就为补全点的词条
                String text = option.getText().string();
                prefixList.add(text);
            }
            return prefixList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ArrayList<String> getAggByName(Aggregations aggregations, String aggName) {
        //4.1根据聚合名称获取聚合结果
        Terms brandTerms = aggregations.get(aggName);
        //4.2获取buckets
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();

        ArrayList<String> brandList = new ArrayList<>();

        //4.3遍历brand buckets
        for (Terms.Bucket bucket : buckets) {
            //4.4获取key
            String key = bucket.getKey().toString();
            //将key放到list中
            brandList.add(key);
            //4.5获取bucket中的文档数量
            long docCount = bucket.getDocCount();
        }
        return brandList;
    }

    private static void buildAggregation(SearchRequest request) {
        //品牌聚合
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(100)
        );
        //城市聚合
        request.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(100)
        );
        //星级聚合
        request.source().aggregation(AggregationBuilders
                .terms("starAgg")
                .field("starName")
                .size(100)
        );
    }


    private static PageResult ResponseHandler(SearchResponse response) {
        ArrayList<HotelDoc> hotelDocs = new ArrayList<>();
        SearchHits responseHits = response.getHits();
        long total = responseHits.getTotalHits().value;
        SearchHit[] hits = responseHits.getHits();
        for (SearchHit hit : hits) {
            String source = hit.getSourceAsString();
            HotelDoc hotelDoc = JSONUtil.toBean(source, HotelDoc.class);
            //获取排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotelDocs.add(hotelDoc);
        }
        return new PageResult(total, hotelDocs);
    }
}
