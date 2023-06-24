package com.tuling.zkqueue.queue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;

import java.io.IOException;

/**
 * 消费者
 */
public class DistributedQueueConsumer {

    public static void main(String[] args) {

        DistributedQueue<String> queue = null;
        try {
            CuratorFramework client = com.tuling.zkqueue.queue.ZKQueueUtils.getClient();

            // 定义队列消费者
            QueueConsumer<String> consumer = new QueueConsumer<String>() {
                @Override
                public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {

                }
                @Override
                public void consumeMessage(String message) {
                    //Map map = JSON.parseObject(message, Map.class);
                    System.out.println(message);
                    // TODO
                }
            };
            // 创建分布式队列
            QueueBuilder<String> builder = QueueBuilder.builder(client,
                    consumer, com.tuling.zkqueue.queue.ZKQueueUtils.createQueueSerializer(), "/order");
            // 指定了一个锁节点路径/orderlock,用于实现分布式锁，以保证队列操作的原子性和顺序性。
            queue = builder.lockPath("/orderlock").buildQueue();
            // 启动队列,这时队列开始监听ZooKeeper中/order节点下的消息。
            queue.start();
            // 阻塞进程
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (queue != null) {
                    queue.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
