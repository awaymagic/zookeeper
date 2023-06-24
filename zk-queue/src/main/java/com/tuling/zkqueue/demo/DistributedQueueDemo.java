package com.tuling.zkqueue.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedQueueDemo {

    private static final String QUEUE_ROOT = "/distributed_queue";
    private final ZooKeeper zk;

    public DistributedQueueDemo(String zkAddress) throws IOException, InterruptedException {
        CountDownLatch connectedSignal = new CountDownLatch(1);

        zk = new ZooKeeper(zkAddress, 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });

        connectedSignal.await();

        try {
            // 判断/distributed_queue节点是否存在
            Stat stat = zk.exists(QUEUE_ROOT, false);
            if (stat == null) {
                //创建持久节点 /distributed_queue
                zk.create(QUEUE_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 入队
     * @param data
     * @throws Exception
     */
    public void enqueue(String data) throws Exception {
        // 创建临时有序子节点
        zk.create(QUEUE_ROOT + "/queue-", data.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    /**
     * 出队
     * @return
     * @throws Exception
     */
    public String dequeue() throws Exception {
        while (true) {
            List<String> children = zk.getChildren(QUEUE_ROOT, false);
            if (children.isEmpty()) {
                return null;
            }

            Collections.sort(children);

            for (String child : children) {
                String childPath = QUEUE_ROOT + "/" + child;
                try {
                    byte[] data = zk.getData(childPath, false, null);
                    zk.delete(childPath, -1);
                    return new String(data, StandardCharsets.UTF_8);
                } catch (KeeperException.NoNodeException e) {
                    // 节点已被其他消费者删除，尝试下一个节点
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        DistributedQueueDemo queue = new DistributedQueueDemo("localhost:2181");

        // 生产者线程
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    // 入队
                    queue.enqueue("Task-" + i);
                    System.out.println("Enqueued: Task-" + i);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        // 消费者线程
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    // 出队
                    String task = queue.dequeue();
                    System.out.println("Dequeued: " + task);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
