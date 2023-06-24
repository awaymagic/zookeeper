package com.tuling.service;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author Fox
 */
public class OrderCodeGenerator {

    private static int count = 0;

    /**
     * 生成订单号
     */
    public String getOrderCode(){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
        return simpleDateFormat.format(new Date()) + "-" + ++count;
    }


}
