package com.tuling.zkqueue.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.*;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorDistributedQueueDemo {

    private static final String QUEUE_ROOT = "/curator_distributed_queue";

    public static void main(String[] args) throws Exception {

        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181",
                new ExponentialBackoffRetry(1000, 3));

        client.start();

        // 定义队列序列化和反序列化
        QueueSerializer<String> serializer = new QueueSerializer<String>() {

            @Override
            public byte[] serialize(String item) {
                return item.getBytes();
            }

            @Override
            public String deserialize(byte[] bytes) {
                return new String(bytes);
            }

        };

        // 定义队列消费者
        QueueConsumer<String> consumer = new QueueConsumer<String>() {
            @Override
            public void consumeMessage(String message) {
                System.out.println("消费消息: " + message);
            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {

            }
        };

        // 创建分布式队列
        DistributedQueue<String> queue = QueueBuilder.builder(client, consumer, serializer, QUEUE_ROOT)
                .buildQueue();
        queue.start();

        // 生产消息
        for (int i = 0; i < 5; i++) {
            String message = "Task-" + i;
            System.out.println("生产消息: " + message);
            queue.put(message);
            Thread.sleep(1000);
        }

        Thread.sleep(10000);
        queue.close();
        client.close();
    }
}
