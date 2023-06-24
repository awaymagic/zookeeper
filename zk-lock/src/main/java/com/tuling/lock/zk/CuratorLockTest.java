package com.tuling.lock.zk;

import com.tuling.service.OrderCodeGenerator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 推荐使用 Curator 框架实现的分布式锁
 *
 * @Author Fox
 */
public class CuratorLockTest implements Runnable {

    final static CuratorFramework CLIENT = CuratorFrameworkFactory.builder().connectString("localhost:2181")
            .retryPolicy(new ExponentialBackoffRetry(100, 1)).build();

    private final OrderCodeGenerator orderCodeGenerator = new OrderCodeGenerator();

    /**
     * 可重入互斥锁
     */
    final InterProcessMutex lock = new InterProcessMutex(CLIENT, "/curator_lock");


    public static void main(String[] args) throws InterruptedException {

        CLIENT.start();

        for (int i = 0; i < 30; i++) {
            new Thread(new CuratorLockTest()).start();
        }

        Thread.currentThread().join();

    }

    @Override
    public void run() {
        try {
            // 加锁
            lock.acquire();

            String orderCode = orderCodeGenerator.getOrderCode();
            System.out.println("生成订单号 " + orderCode);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // 释放锁
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
