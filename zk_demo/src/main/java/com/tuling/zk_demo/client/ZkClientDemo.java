package com.tuling.zk_demo.client;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

/**
 * @Author Fox
 */
public class ZkClientDemo {

    // private static final  String  CONNECT_STR="192.168.65.204:2181";
    // private final static String CLUSTER_CONNECT_STR = "localhost:2181,192.168.65.184:2181,192.168.65.186:2181";
    private final static String CLUSTER_CONNECT_STR = "localhost:2181";

    public static void main(String[] args) throws Exception {
        // 获取zookeeper对象
        ZooKeeper zooKeeper = ZooKeeperFacotry.create(CLUSTER_CONNECT_STR);

        // CONNECTED
        System.out.println(zooKeeper.getState());

        // 是否存在节点
        Stat stat = zooKeeper.exists("/user", false);
        if (null == stat) {
            // 创建持久节点(持久 持久有序 临时 临时有序 容器 ttl节点)
            zooKeeper.create("/user", "fox".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 永久监听 相当于 addWatch -m mode /user
        zooKeeper.addWatch("/user", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
                // TODO
            }
            // 持久/持久递归
        }, AddWatchMode.PERSISTENT);


        // 写入状态信息
        stat = new Stat();
        byte[] data = zooKeeper.getData("/user", false, stat);
        System.out.println(" data: " + new String(data));
        // -1: 无条件更新
        // zooKeeper.setData("/user", "third".getBytes(), -1);
        // 带版本条件更新
        int version = stat.getVersion();

        zooKeeper.setData("/user", "fox".getBytes(), version);

        Thread.sleep(Integer.MAX_VALUE);
    }

}
