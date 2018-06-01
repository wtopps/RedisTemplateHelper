package com.xuangy.redis.helper.cache;

import com.xuangy.redis.helper.jedis.RedisHelperJedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * RedisTemplate operate helper, convenient to convert JavaBean to JSON, then store into redisHelper,
 * to rebecome JavaBean when you want to get it from Redis.
 *
 * @author xuanguangyao
 *
 * @version 1.0, 2018/5/29
 *
 */
public class RedisTemplate extends StringRedisTemplate {

    public static ThreadLocal<Integer> LOCAL_DB_INDEX = new ThreadLocal<>();

    @Override
    protected RedisConnection preProcessConnection(RedisConnection connection, boolean existingConnection) {
        try {
            Integer dbIndex = LOCAL_DB_INDEX.get();
            //如果设置了dbIndex
            if (dbIndex != null) {
                if (connection instanceof RedisHelperJedisConnection) {
                    if (((RedisHelperJedisConnection) connection).getDbIndex() != dbIndex) {
                        connection.select(dbIndex);
                    } else {
                        System.out.println("no selectdb");
                    }
                } else {
                    connection.select(dbIndex);
                }
            }
        } finally {
            LOCAL_DB_INDEX.remove();
        }

        return super.preProcessConnection(connection, existingConnection);
    }
}
