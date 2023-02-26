package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Resource
    private IHotelService hotelService;

    @PostMapping("/list")
    public PageResult GetHotels(@RequestBody RequestParams requestParams) {
        return hotelService.getHotels(requestParams);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> getFilters(@RequestBody RequestParams params) {
        return hotelService.filters(params);
    }

    @GetMapping("suggestion")
    public List<String> getSuggestions(@RequestParam String key) {
        return hotelService.getSuggestions(key);
    }

}
