package com.tuling.zk_demo.curator.namingserver;


import com.tuling.zk_demo.curator.CuratorBaseOperations;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;


@EqualsAndHashCode(callSuper = true)
@Data
public class SnowflakeIdWorker extends CuratorBaseOperations{

    // Zk客户端
    transient private CuratorFramework zkClient;

    // 工作节点的路径
    private String pathPrefix = "/test/idmaker/worker-";
    private String pathRegistered = null;

    public static SnowflakeIdWorker instance = new SnowflakeIdWorker();

    private SnowflakeIdWorker() {
        this.init();
        this.zkClient = getCuratorFramework();
        this.initData();
    }


    // 在zookeeper中创建临时节点并写入信息
    public void initData() {

        // 创建一个 ZNode 节点
        // 节点的 payload 为当前worker 实例
        try {
            byte[] payload = pathPrefix.getBytes();
            pathRegistered = zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(pathPrefix, payload);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public long getId() {
        String sid = null;
        if (null == pathRegistered) {
            throw new RuntimeException("节点注册失败");
        }
        int index = pathRegistered.lastIndexOf(pathPrefix);
        if (index >= 0) {
            index += pathPrefix.length();
            sid = index <= pathRegistered.length() ? pathRegistered.substring(index) : null;
        }

        if (null == sid) {
            throw new RuntimeException("节点ID生成失败");
        }

        return Long.parseLong(sid);

    }
}