package com.tuling.zk_demo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.UnsupportedEncodingException;

/**
 * @Author Fox
 */
public class JsonUtil {

    public static  <T> T jsonBytes2Object(byte[] bytes, Class<T> tClass){
        //字节数组，转成JSON字符串
        try {
            String json = new String(bytes, "UTF-8");
            T t = JSONObject.parseObject(json, tClass);
            return t;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] object2JsonBytes(Object obj) {
        //把对象转换成JSON
        String json = JSON.toJSONString(obj);
        try
        {
            return json.getBytes("UTF-8");
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }
}
