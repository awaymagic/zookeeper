package com.tuling.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author Fox
 *
 */
public class OrderService implements Runnable {

    private OrderCodeGenerator orderCodeGenerator = new OrderCodeGenerator();

    // 闭锁，模拟30个并发
    private static CountDownLatch countDownLatch = new CountDownLatch(30);

    private static Lock lock = new ReentrantLock();

    @Override
    public void run() {

        lock.lock(); // 加锁

        try {
            System.out.println("生成订单号 " + orderCodeGenerator.getOrderCode());
            countDownLatch.countDown(); // 减1
        } finally {
            lock.unlock(); // 释放锁
        }

    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("-----生成唯一的订单号----------");

        for (int i = 0; i < 30; i++) {
            new Thread(new OrderService()).start();
        }
        countDownLatch.await();  // 阻塞  计数器0

        Thread.currentThread().join();

    }
}
