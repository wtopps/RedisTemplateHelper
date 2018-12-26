package com.xuangy.redis.helper;

import com.xuangy.redis.helper.cache.RedisTemplateHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by xuanguangyao on 2018/6/1.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@EnableAutoConfiguration
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class RedisTemplateHelperTest {

    @Autowired
    private RedisTemplateHelper redisTemplateHelper;

    @Test
    public void testRedisTemplateOperate() {

    }
}
