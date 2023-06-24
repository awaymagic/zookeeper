package com.tuling.zk_demo.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * CuratorDemo
 * @Author Fox
 */
public class CuratorDemo {

    // private final static String CLUSTER_CONNECT_STR = "192.168.65.163:2181,192.168.65.184:2181,192.168.65.186:2181";
    private final static String CLUSTER_CONNECT_STR = "localhost:2181";

    public static void main(String[] args) throws Exception {
        // 构建客户端实例
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(CLUSTER_CONNECT_STR)
                // 设置重试策略 每次重试机会增加
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        // 启动客户端
        curatorFramework.start();

        String path = "/user";

        // 检查节点是否存在
        Stat stat = curatorFramework.checkExists().forPath(path);
        if (stat != null) {
            // 删除节点
            curatorFramework.delete()
                    // 如果存在子节点，则删除所有子节点
                    .deletingChildrenIfNeeded()
                    // 删除指定节点
                    .forPath(path);
        }
        // 创建节点
        curatorFramework.create()
                .creatingParentsIfNeeded()  // 如果父节点不存在，则创建父节点(带层级结构的节点)
                .withMode(CreateMode.PERSISTENT) // 永久节点
                .forPath(path, "Init Data".getBytes());

        // 注册节点监听
        curatorFramework.getData()
                .usingWatcher((CuratorWatcher) event -> {
                    byte[] bytes = curatorFramework.getData().forPath(path);
                    // 监听到
                    System.out.println("Node data changed: " + new String(bytes));
                })
                .forPath(path);


        // 更新节点数据    set /user  Update Data
        curatorFramework.setData()
                .forPath(path, "Update Data".getBytes());


        stat = new Stat();
        // 查询节点数据
        byte[] bytes = curatorFramework.getData().storingStatIn(stat)
                .forPath(path);
        System.out.println(new String(bytes));


        ExecutorService executorService = Executors.newSingleThreadExecutor();
        // 异步处理，可以指定线程池
        // item1对象 item2里边的数据 (getData之后,异步执行inBackground里代码逻辑)
        curatorFramework.getData().inBackground((item1, item2) -> {
            System.out.println("background:" + item1 + "," + item2);
            System.out.println(item2.getStat());
        }, executorService).forPath(path);


        // 创建节点缓存,用于监听指定节点的变化
        final NodeCache nodeCache = new NodeCache(curatorFramework, path);
        // 如果是子节点
        // 启动NodeCache并立即从服务端获取最新数据
        nodeCache.start(true);

        // 注册节点变化监听器
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                byte[] newData = nodeCache.getCurrentData().getData();
                // 打印
                System.out.println("Node data changed: " + new String(newData));
            }
        });

        // 创建PathChildrenCache
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, path, true);
        pathChildrenCache.start();

        // 注册子节点变化监听器
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                    ChildData childData = event.getData();
                    System.out.println("Child added: " + childData.getPath());
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    ChildData childData = event.getData();
                    System.out.println("Child removed: " + childData.getPath());
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                    ChildData childData = event.getData();
                    System.out.println("Child updated: " + childData.getPath());
                }
            }
        });


        Thread.sleep(Integer.MAX_VALUE);

    }
}
