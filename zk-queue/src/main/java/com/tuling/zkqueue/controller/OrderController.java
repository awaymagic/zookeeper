package com.tuling.zkqueue.controller;

import com.alibaba.fastjson.JSON;
import com.tuling.zkqueue.queue.ZKQueueUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 测试时先创建 /order/create
 * 后启动消费者 DistributedQueueConsumer
 */
@RestController
@RequestMapping("/order")
public class OrderController {

    private static final int COUNT = 1000;

    @PostMapping("/create")
    public String createOrder(){
        // 模拟下单
        for (int i = 0; i < COUNT; i++) {
            Map<Object, Object> map = new HashMap<>();
            map.put("orderId", UUID.randomUUID().toString());
            map.put("price", COUNT * i);
            map.put("dataTime", new Date());
            map.put("seq", i);
            // 订单对象存入队列
            ZKQueueUtils.setQueueData(JSON.toJSONString(map));
        }
        return "success";
    }
}
