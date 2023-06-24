package com.tuling.lock.zk;

import com.tuling.lock.Lock;
import com.tuling.service.OrderCodeGenerator;


/**
 * @Author Fox
 *
 */
public class ZKLockTest implements Runnable{

    private final OrderCodeGenerator orderCodeGenerator = new OrderCodeGenerator();

    // private final Lock lock = new DistributedLock();
    private final Lock lock = new DistributedLockByEPHEMERAL();

    @Override
    public void run() {

        lock.lock();
        try {
            String orderCode = orderCodeGenerator.getOrderCode();
            System.out.println("生成订单号 " + orderCode);
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) throws InterruptedException {

        for (int i = 0; i < 30; i++) {
            new Thread(new ZKLockTest()).start();
        }

        Thread.currentThread().join();
    }


}
