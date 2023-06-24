package com.tuling.zk_demo.curator.namingserver;

import com.tuling.zk_demo.curator.CuratorBaseOperations;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

/**
 * @Author Fox
 */
@Slf4j
public class IDMaker extends CuratorBaseOperations {

    private String createSeqNode(String pathPefix) throws Exception {
        CuratorFramework curatorFramework = getCuratorFramework();
        //创建一个临时顺序节点
        return curatorFramework.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(pathPefix);
    }

    public String makeId(String path) throws Exception {
        String str = createSeqNode(path);
        if(null != str){
            //获取末尾的序号
            int index = str.lastIndexOf(path);
            if (index >= 0) {
                index += path.length();
                return index <= str.length() ? str.substring(index) : "";
            }
        }
        return str;
    }


}
