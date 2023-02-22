package cn.itcast.hotel;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import cn.itcast.hotel.service.impl.HotelService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@SpringBootTest
public class HotelDocTest {

    private RestHighLevelClient client;

    @Resource
    private IHotelService hotelService;


    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://10.38.128.128:9200")
        ));
    }


    //创建doc文档,关键api:client.index()
    @Test
    void testCreateDoc() throws IOException {
        //1.创建request对象
        IndexRequest request = new IndexRequest("hotel");

        //2.获取数据库中的文件
        List<Hotel> hotels = hotelService.list();
        for (Hotel hotel : hotels) {
            //2.1将hotel类型转换为HotelDoc,通过构造函数转换,将两个经纬度字段合成为一个
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //2.2将bean转换为json文件
            String s = JSONUtil.toJsonStr(hotelDoc);
            //3.将request.source(json文档,类型)
            request.id(hotelDoc.getId().toString()).source(s, XContentType.JSON);
            //4.发送请求,文档的操作为client.index()
            client.index(request, RequestOptions.DEFAULT);
        }
    }

    //查询doc文档,关键api:client.get()
    @Test
    void testGetDoc() throws IOException {
        List<Hotel> hotels = hotelService.list();

        List<Long> ids = hotels.stream().map(Hotel::getId).collect(Collectors.toList());

        for (Long id : ids) {
            //1.创建请求
            GetRequest get = new GetRequest("hotel", id.toString());
            //2.发送请求,并获取结果
            GetResponse response = client.get(get, RequestOptions.DEFAULT);
            //3.解析结果
            Map<String, Object> source = response.getSource();
            //4.打印结果
            HotelDoc hotelDoc = BeanUtil.fillBeanWithMap(source, new HotelDoc(), false);
            System.out.println(hotelDoc);
        }

    }

    //更新文档,关键api:client.update()
    @Test
    void testUpdateDocById() throws IOException {
        //1.创建request对象
        UpdateRequest request = new UpdateRequest("hotel", "36934");
        //2.封装request对象,每两个参数为一对
        request.doc(
                "name", "蔡徐坤连锁酒店",
                "price", 250
        );
        //3.更新文档
        client.update(request, RequestOptions.DEFAULT);
    }


    @AfterEach
    void close() throws IOException {
        this.client.close();
    }

}
