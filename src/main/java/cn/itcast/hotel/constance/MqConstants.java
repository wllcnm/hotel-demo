package cn.itcast.hotel.constance;

public class MqConstants {


    //交换机,类型为topic
    public static final String HOTEL_EXCHANGE = "hotel.topic";

    //监听插入的队列
    public static final String HOTEL_INSERT_QUEUE = "hotel.insert.queue";

    //删除的队列
    public static final String HOTEL_DELETE_QUEUE = "hotel.delete.queue";

    //新增或修改的RoutingKey
    public static final String HOTEL_INSERT_KEY = "hotel.insert";

    //删除的RoutingKey
    public static final String HOTEL_DELETE_KEY = "hotel.delete";
}
