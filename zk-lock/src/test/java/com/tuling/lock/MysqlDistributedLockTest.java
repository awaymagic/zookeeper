package com.tuling.lock;

import com.tuling.lock.sql.MysqlDistributedLock;
import com.tuling.service.OrderCodeGenerator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:/spring-mybatis.xml"})
public class MysqlDistributedLockTest {

    private  OrderCodeGenerator orderCodeGenerator = new OrderCodeGenerator();

    @Autowired
    private MysqlDistributedLock lock;

    public  void getOrderCode(){
        //加锁的本质： 保证多线程对共享资源的序列化访问
        lock.lock();
        try {
            String orderCode = orderCodeGenerator.getOrderCode();
            System.out.println("生成订单号 "+orderCode);
        }finally {
            lock.unlock();
        }
    }

    @Test
    public void test() throws InterruptedException {

        for(int i=0;i<10;i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    getOrderCode();
                }
            }).start();
        }

        Thread.currentThread().join();
    }


}
