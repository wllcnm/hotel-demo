package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constance.Hotel.HOTEL_TEMP;

public class HotelIndexTest {

    private RestHighLevelClient client;


    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://10.38.128.128:9200")
        ));
    }

    @Test
    void testInit() {
        System.out.println(client);
    }

    //1.创建索引库
    @Test
    void testCreateHotelIndex() throws IOException {
        //1.创建request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");

        //2.请求参数
        request.source(HOTEL_TEMP, XContentType.JSON);

        //3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    //2.删除索引库
    @Test
    void testDeleteHotelIndex() throws IOException {
        //1.创建删除对象
        DeleteIndexRequest delete = new DeleteIndexRequest("hotel");

        //2.发送删除请求
        client.indices().delete(delete, RequestOptions.DEFAULT);
    }

    //3.判断索引库是否存在
    @Test
    void testIsExistIndex() throws IOException {
        //1.创建get请求
        GetIndexRequest get = new GetIndexRequest("hotel");
        //2.发送请求
        boolean isExists = client.indices().exists(get, RequestOptions.DEFAULT);

        System.out.println(isExists ? "索引库存在" : "索引库不存在!");
    }

    @AfterEach
    void close() throws IOException {
        this.client.close();
    }
}
