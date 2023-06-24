package com.tuling.lock.zk;

import com.tuling.lock.AbstractLock;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class DistributedLockByEPHEMERAL extends AbstractLock {

    private static final String connectString = "localhost:2181";
    private static final int sessionTimeout = 5000;
    private static final String LOCK_PATH = "/lock";
    private ZooKeeper zooKeeper;
    private CountDownLatch lockAcquiredSignal = new CountDownLatch(1);


    public DistributedLockByEPHEMERAL()  {
        try {
            CountDownLatch connectLatch = new CountDownLatch(1);
            zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // 连接建立时, 打开latch, 唤醒wait在该latch上的线程
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        connectLatch.countDown();
                    }

                    // 发生了waitPath的删除事件
                    if (event.getType() == Event.EventType.NodeDeleted
                            && event.getPath().equals(LOCK_PATH)) {
                        lockAcquiredSignal.countDown();
                    }
                }
            });
            // 等待连接建立
            connectLatch.await();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public boolean tryLock() {
        try {
            // 创建临时节点/lock
            zooKeeper.create(LOCK_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e){
            // 节点已经存在，创建失败
            return false;
        }

        return true;
    }

    @Override
    public void waitLock() {
        try {
            // 判断是否存在，监听节点
            Stat stat = zooKeeper.exists(LOCK_PATH, true);
            if(null != stat){
                // 存在阻塞在此
                lockAcquiredSignal.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void unlock()  {
        try {
            // 删除临时节点
            zooKeeper.delete(LOCK_PATH, -1);
            System.out.println("-------释放锁------");
        } catch (Exception e) {

        }
    }

}