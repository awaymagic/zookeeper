package com.tuling.zkqueue.queue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZKQueueUtils {

    final static CuratorFramework CLIENT;
    private static DistributedQueue<String> QUEUE;
    private static final String QUEUE_NAME = "/order";
 
    static {
        // 初始化连接
        CLIENT = CuratorFrameworkFactory.builder().connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(100,1)).build();
        CLIENT.start();
        // 创建队列
        createQueue();
    }

    public static CuratorFramework getClient(){
        return ZKQueueUtils.CLIENT;
    }

    public static void closeClient(){
        CLIENT.close();
    }
 
    /**
     * 创建队列
     * @param
     */
    public static void createQueue(){
        QueueBuilder<String> builder = QueueBuilder.builder(CLIENT, null, createQueueSerializer(), QUEUE_NAME);
        QUEUE = builder.buildQueue();
        try {
            QUEUE.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 创建队列
     * @param
     */
    public static DistributedQueue getQUEUE(){
        return QUEUE;
    }
 
    public static void setQueueData(String data){
        try {
            QUEUE.put(data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void closeQueue(){
        try {
            QUEUE.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 
    public static QueueSerializer<String> createQueueSerializer() {
        return new QueueSerializer<String>(){

            @Override
            public byte[] serialize(String item) {
                return item.getBytes();
            }

            @Override
            public String deserialize(byte[] bytes) {
                return new String(bytes);
            }

        };
    }

}
