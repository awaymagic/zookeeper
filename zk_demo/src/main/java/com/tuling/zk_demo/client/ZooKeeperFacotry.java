package com.tuling.zk_demo.client;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

public class ZooKeeperFacotry {

    private static final int SESSION_TIMEOUT = 5000;

    public static ZooKeeper create(String connectionString) throws Exception {
        final CountDownLatch connectionLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(connectionString, SESSION_TIMEOUT, event -> {
            if (event.getType()== Watcher.Event.EventType.None
                    && event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                // 和服务端建立连接完成
                connectionLatch.countDown();
                System.out.println("连接建立");
            }
        });

        System.out.println("等待连接建立...");
        connectionLatch.await();

        return zooKeeper;
    }

}
