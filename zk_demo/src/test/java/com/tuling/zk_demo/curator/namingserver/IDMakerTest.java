package com.tuling.zk_demo.curator.namingserver;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * 测试 Id 生成器
 * @Author Fox
 */
@Slf4j
public class IDMakerTest {

    @Test
    public void testMarkId() throws Exception {
        IDMaker idMaker = new IDMaker();
        idMaker.init();
        String pathPrefix = "/idmarker/id-";
        // 模拟5个线程创建id
        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                for (int j = 0; j < 10; j++) {
                    String id;
                    try {
                        id = idMaker.makeId(pathPrefix);
                        log.info("线程{}第{}次创建id为{}", Thread.currentThread().getName(), j, id);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, "thread" + i).start();
        }

        Thread.sleep(Integer.MAX_VALUE);

    }
}
