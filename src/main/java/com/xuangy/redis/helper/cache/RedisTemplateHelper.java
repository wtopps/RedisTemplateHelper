package com.xuangy.redis.helper.cache;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionCommands;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * RedisTemplate operate helper, convenient to convert JavaBean to JSON, then store into redisHelper,
 * to rebecome JavaBean when you want to get it from Redis.
 *
 * @author xuanguangyao
 *
 * @version 1.0, 2018/5/29
 *
 */

public class RedisTemplateHelper {

    private static final Logger logger = LoggerFactory.getLogger(RedisTemplateHelper.class);

//    private ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private RedisTemplate redisTemplate;

    public Boolean set(int dbIndex, String key, String value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.opsForValue().set(key, value);
        return true;
    }

    public Boolean setnx(int dbIndex, String key, String value) {
        try {
            RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
            return redisTemplate.opsForValue().setIfAbsent(key, value);
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.setnx error, key=" + key + " value=" + value, e);
        }
        return false;
    }

    public Boolean setex(int dbIndex, String key, String value, long timeout, TimeUnit timeUnit) {
        boolean result = true;
        try {
            RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
            redisTemplate.opsForValue().set(key, value, timeout, timeUnit);
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.setex error, key=" + key + " value=" + value, e);
            result = false;
        }
        return result;
    }

    public String get(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForValue().get(key);
    }

    public Boolean delete(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.delete(key);
        return true;
    }

    public Boolean delete(int dbIndex, Set<String> keys) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.delete(keys);
        return true;
    }

    public Boolean expire(int dbIndex, String key, long timeout, TimeUnit unit) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.expire(key, timeout, unit);
        return true;
    }

    @SuppressWarnings("unchecked")
    public Long ttl(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return (Long) redisTemplate.execute((RedisCallback)(connection) -> connection.ttl(((RedisSerializer<String>) redisTemplate.getKeySerializer()).serialize(key)));
    }

    public <T> Boolean set(int dbIndex, String key, T t) {
        boolean result = true;
        try {
            String json = JSON.toJSONString(t);
            RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
            redisTemplate.opsForValue().set(key, json);
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.set error, key=" + key + " value=" + t, e);
            result = false;
        }
        return result;
    }

    public<T> Boolean setnx(int dbIndex, String key, T t) {
        try {
            String json = JSON.toJSONString(t);
            RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
            return redisTemplate.opsForValue().setIfAbsent(key, json);
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.setnx error, key=" + key + " value=" + t, e);
        }
        return false;
    }

    public<T> Boolean setex(int dbIndex, String key, T t, long timeout, TimeUnit timeUnit) {
        boolean result = true;
        try {
            String json = JSON.toJSONString(t);
            RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
            redisTemplate.opsForValue().set(key, json, timeout, timeUnit);
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.setnx error, key=" + key + " value=" + t, e);
            result = false;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public <T> Boolean batchSet(int dbIndex, Map<String, T> map) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.execute((RedisCallback)(connection)-> {
            for (Map.Entry<String, T> entry : map.entrySet()) {
                String json;
                try {
                    json = JSON.toJSONString(entry.getValue());
                } catch (Exception e) {
                    continue;
                }
                try {
                    connection.set(((RedisSerializer<String>) redisTemplate.getKeySerializer()).serialize(entry.getKey()),
                            ((RedisSerializer<String>) redisTemplate.getValueSerializer()).serialize(json));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.batchSet error", e);
                }
            }
            return null;
        });
        return true;
    }

    public  Map<String, String> batchGet(int dbIndex, Map<String, String> map) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, String> resultMap = new HashMap<>();
        List<Object> resultList  = redisTemplate.executePipelined((RedisCallback)(connection)->{
            for (Map.Entry<String, String> entry : map.entrySet()) {
                try {
                    connection.get(redisTemplate.getStringSerializer().serialize(entry.getValue()));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.batchGet error", e);
                }
            }
            return null;
        });

        if (resultList != null && resultList.size() == map.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Object result = resultList.get(i);
                if (result != null && !StringUtils.isEmpty(result.toString())) {
                    try {
                        resultMap.put(entry.getKey(), result.toString());
                    } catch (Exception e) {
                        logger.error("RedisTemplateHelper.batchGet error", e);
                    }
                }
                i++;
            }
        }
        return resultMap;
    }

    public <T> Map<String, T> batchGet(int dbIndex, Map<String, String> map, Class<T> clazz) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, T> resultMap = new HashMap<>();
        List<Object> resultList  = redisTemplate.executePipelined((RedisCallback)(connection)->{
            for (Map.Entry<String, String> entry : map.entrySet()) {
                try {
                    connection.get(redisTemplate.getStringSerializer().serialize(entry.getValue()));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.batchGet error", e);
                }
            }
            return null;
        });

        if (resultList != null && resultList.size() == map.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Object result = resultList.get(i);
                if (result != null && !StringUtils.isEmpty(result.toString())) {
                    try {
                        resultMap.put(entry.getKey(), JSON.parseObject(result.toString(), clazz));
                    } catch (Exception e) {
                        logger.error("RedisTemplateHelper.batchGet error", e);
                    }
                }
                i++;
            }
        }
        return resultMap;
    }

    /**
     * 批量获取一组List
     * 参数中map的key是返回值resultMap的key, value是redis缓存的key
     */
    public <T> Map<String, List<T>> batchGetList(int dbIndex, Map<String, String> map, Class<T> clazz) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, List<T>> resultMap = new HashMap<>();
        List<Object> resultList  = redisTemplate.executePipelined((RedisCallback)(connection)-> {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                try {
                    connection.get(redisTemplate.getStringSerializer().serialize(entry.getValue()));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.batchGetList error", e);
                }
            }
            return null;
        });

        if (resultList != null && resultList.size() == map.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Object result = resultList.get(i);
                if (result != null && !StringUtils.isEmpty(result.toString())) {
                    try {
//                        JavaType valueType = mapper.getTypeFactory()
//                                .constructParametrizedType(ArrayList.class, List.class, clazz);
                        List<T> list = JSON.parseArray(result.toString(), clazz);
                        resultMap.put(entry.getKey(), list);
                    } catch (Exception e) {
                        logger.error("RedisTemplateHelper.batchGetList error", e);
                    }
                }
                i++;
            }
        }
        return resultMap;
    }

    public <T> T get(int dbIndex, String key, Class<T> clazz) {
        T t = null;
        try {
            RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
            String value = redisTemplate.opsForValue().get(key);
            if (StringUtils.isEmpty(value)) {
                return null;
            }

            t = JSON.parseObject(value, clazz);
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.get error", e);
        }
        return t;
    }

    public <T> List<T> getList(int dbIndex, String key, Class<T> clazz) {
        List<T> list = null;
        try {
            RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
            String value = redisTemplate.opsForValue().get(key);
            if (StringUtils.isEmpty(value)) {
                return new ArrayList<>();
            }

//            JavaType valueType = mapper.getTypeFactory()
//                    .constructParametrizedType(ArrayList.class, List.class, clazz);
//            list = mapper.readValue(value, valueType);
            list = JSON.parseArray(value, clazz);
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.getList error", e);
        }
        return list;
    }

    public Set<String> scan(int dbIndex, String pattern) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        ScanOptions scanOptions = ScanOptions.scanOptions().match(pattern).build();
        return redisTemplate.execute((RedisCallback<Set<String>>) connection -> {
            boolean done = false;
            Set<String> keySet = new HashSet<String>();
            // the while-loop below makes sure that we'll get a valid cursor -
            // by looking harder if we don't get a result initially
            while (!done) {
                Cursor<byte[]> c = connection.scan(scanOptions);
                try {
                    while (c.hasNext()) {
                        byte[] b = c.next();
                        keySet.add(new String(b));
                    }
                    done = true; //we've made it here, lets go away
                } catch (NoSuchElementException nse) {
                    logger.error("RedisTemplateHelper.scan error", nse);
                }
            }
            return keySet;
        });
    }

    public Long incrby(int dbIndex, String key, long delta) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForValue().increment(key, delta);
    }

    /**
     * 发布消息到Channel
     * @param channel
     * @param message
     * @param <T>
     * @return
     */
    public <T> Boolean convertAndSend(String channel, T message) {
        try {
            String json = JSON.toJSONString(message);
            //logger.info("[convertAndSend] " + channel + " " + json);
            redisTemplate.convertAndSend(channel, json);
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.convertAndSend error, channel=" + channel + " message=" + message, e);
        }

        return true;
    }

    public <T> T execute(int dbIndex, SessionCallback<T> session) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.execute(session);
    }

    public <T> T execute(int dbIndex, RedisCallback<T> redisCallback) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.execute(redisCallback);
    }

    /**
     * TIME
     * Return the current server time
     * @return 时间戳
     */
    public Long time() {
        return redisTemplate.execute((RedisCallback<Long>) RedisServerCommands::time);
    }

    /**
     * PING
     */
    public String ping() {
        return redisTemplate.execute((RedisCallback<String>) RedisConnectionCommands::ping);
    }

    // ops for list
    /**
     * LPOP key
     * Remove and get the first element in a list
     * @param dbIndex 数据库index
     * @param key 键
     * @return list中的第一个元素
     */
    public String lpop(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().leftPop(key);
    }

    /**
     * BLPOP key [key ...] timeout
     * Remove and get the first element in a list, or block until one is available
     * @param dbIndex 数据库index
     * @param key 键
     * @param timeout 超时时间
     * @param timeUnit 时间单位
     * @return list中的第一个元素
     */
    public String blpop(int dbIndex, String key, long timeout, TimeUnit timeUnit) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().leftPop(key, timeout, timeUnit);
    }

    /**
     * RPOP key
     * Remove and get the last element in a list
     * @param dbIndex 数据库index
     * @param key 键
     * @return list中最后一个元素
     */
    public String rpop(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().rightPop(key);
    }

    /**
     * BRPOP key [key ...] timeout
     * Remove and get the last element in a list, or block until one is available
     * @param dbIndex 数据库index
     * @param key 键
     * @param timeout 超时时间
     * @param timeUnit 时间单位
     * @return list中最后一个元素
     */
    public String brpop(int dbIndex, String key, long timeout, TimeUnit timeUnit) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().rightPop(key, timeout, timeUnit);
    }

    /**
     * RPOPLPUSH source destination
     * Remove the last element in a list, prepend it to another list and return it
     * @param dbIndex
     * @param sourceKey
     * @param destinationKey
     * @return
     */
    public String rpoplpush(int dbIndex, String sourceKey, String destinationKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey);
    }

    /**
     * BRPOPLPUSH source destination timeout
     * Pop a value from a list, push it to another list and return it; or block until one is available
     * @param dbIndex
     * @param sourceKey
     * @param destinationKey
     * @param timeout
     * @param timeUnit
     * @return
     */
    public String brpoplpush(int dbIndex, String sourceKey, String destinationKey, long timeout, TimeUnit timeUnit) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey, timeout, timeUnit);
    }

    /**
     * LINDEX key index
     * Get an element from a list by its index
     * @param dbIndex
     * @param key
     * @param index
     * @return
     */
    public String lindex(int dbIndex, String key, long index) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().index(key, index);
    }

    /**
     * LINSERT key BEFORE|AFTER pivot value
     * Insert an element before or after another element in a list
     * @param dbIndex
     * @param key
     * @param pivot
     * @param value
     * @return
     */
    public Long linsert(int dbIndex, String key, String pivot, String value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().leftPush(key, pivot, value);
    }

    /**
     * LLEN key
     * Get the length of a list
     * @param dbIndex
     * @param key
     * @return
     */
    public Long llen(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().size(key);
    }

    /**
     * LPUSH key value [value ...]
     * Prepend one or multiple values to a list
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public Long lpush(int dbIndex, String key, String value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().leftPush(key, value);
    }

    /**
     * LPUSHAHALL key value [value ...]
     * Prepend one or multiple values to a list
     * @param dbIndex
     * @param key
     * @param values
     * @return
     */
    public Long lpushall(int dbIndex, String key, String... values) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().leftPushAll(key, values);
    }

    /**
     * LPUSH key value [value ...]
     * Prepend one or multiple values to a list
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public Long lpushx(int dbIndex, String key, String value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().leftPushIfPresent(key, value);
    }

    /**
     * LRANGE key start stop
     * Get a range of elements from a list
     * @param dbIndex
     * @param key
     * @param start
     * @param end
     * @return
     */
    public List<String> lrange(int dbIndex, String key, long start, long end) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().range(key, start, end);
    }

    /**
     * LREM key count value
     * Remove elements from a list
     * @param dbIndex
     * @param key
     * @param count
     * @param value
     * @return
     */
    public Long lrem(int dbIndex, String key, long count, String value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().remove(key, count, value);
    }

    /**
     * LTRIM key start stop
     * Trim a list to the specified range
     * @param dbIndex
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Boolean ltrim(int dbIndex, String key, long start, long end) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.opsForList().trim(key, start, end);
        return true;
    }

    /**
     * LSET key index value
     * Set the value of an element in a list by its index
     * @param dbIndex
     * @param key
     * @param index
     * @param value
     * @return
     */
    public Boolean lset(int dbIndex, String key, long index, String value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.opsForList().set(key, index, value);
        return true;
    }

    /**
     * RPUSH key value [value ...]
     * Append one or multiple values to a list
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public Long rpush(int dbIndex, String key, String value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().rightPush(key, value);
    }

    /**
     * RPUSHX key value
     * Append a value to a list, only if the list exists
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public Long rpushx(int dbIndex, String key, String value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForList().rightPushIfPresent(key, value);
    }


    // ops for hash
    /**
     * HDEL key field [field ...]
     * Delete one or more hash fields
     * @param dbIndex
     * @param key
     * @param hashKeys
     * @return
     */
    public Long hdel(int dbIndex, String key, Object...hashKeys) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().delete(key, hashKeys);
    }

    /**
     * HEXISTS key field
     * Determine if a hash field exists
     * @param dbIndex
     * @param key
     * @param hashKey
     * @return
     */
    public Boolean hexists(int dbIndex, String key, Object hashKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().hasKey(key, hashKey);
    }

    /**
     * HGET key field
     * Get the value of a hash field
     * @param dbIndex
     * @param key
     * @param hashKey
     * @return
     */
    public Object hget(int dbIndex, String key, Object hashKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().get(key, hashKey);
    }

    /**
     * HGET key field
     * then convert to object
     * @param dbIndex
     * @param key
     * @param hashKey
     * @param clazz
     * @return
     */
    public <T> T hget(int dbIndex, String key, Object hashKey, Class<T> clazz) {
        T t = null;
        try {
            RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);

            Object value = redisTemplate.opsForHash().get(key, hashKey);
            if (value == null || StringUtils.isEmpty(value.toString())) {
                return null;
            }

            t = JSON.parseObject(value.toString(), clazz);
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.hget error", e);
        }
        return t;
    }

    /**
     * HGETALL key
     * Get all the fields and values in a hash
     * @param dbIndex
     * @param key
     * @return
     */
    public Map<Object, Object> hgetall(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().entries(key);
    }

    /**
     * Batch HGETALL key
     * Batch operate get all the fields and values in a hash
     * @param dbIndex
     * @param map
     * @return
     */
    public Map<String, Map<String, String>> multihgetall(int dbIndex, Map<String, String> map) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);

        Map<String, Map<String, String>> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                try {
                    connection.hGetAll(redisTemplate.getStringSerializer().serialize(entry.getValue()));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multihgetall error", e);
                }
            }
            return null;
        });
        if (resultList != null && resultList.size() == map.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Map<String, String> result = (Map<String, String>) resultList.get(i);
                if (result != null) {
                    resultMap.put(entry.getKey(), result);
                }
                i++;
            }
        }
        return resultMap;
    }

	/**
	 * HGETALL key
     * Get all the fields and values in a hash, then convert object to list
	 * @param dbIndex
	 * @param key
	 * @param clazz
	 * @return
	 */
    public <T> List<T> hgetList(int dbIndex, String key, Class<T> clazz) {
        List<T> list = null;
        try {
            RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
            Map<Object, Object> map = redisTemplate.opsForHash().entries(key);
            if(map != null){
    	    	list = new ArrayList<T>();
    	    	for(Object mapKey: map.keySet()){
    	    		String jsonStr = map.get(mapKey).toString();
    	    		if(StringUtils.isNotEmpty(jsonStr)){
    	    			T t = JSON.parseObject(jsonStr, clazz);
    	    			list.add(t);
    	    		}
    	    	}
        	}
        } catch (Exception e) {
            logger.error("RedisTemplateHelper.hgetList error", e);
        }
        return list;
    }

    /**
     * Batch HGETALL key
     * Batch operate get all the fields and values in a hash, then convert to object
     * @param dbIndex
     * @param map
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> Map<String,List<T>> multihgetList(int dbIndex, Map<String, String> map, Class<T> clazz) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String,List<T>> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                try {
                    connection.hGetAll(redisTemplate.getStringSerializer().serialize(entry.getValue()));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multihgetList error", e);
                }
            }
            return null;
        });
        if (resultList != null && resultList.size() == map.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Map<String, String> result = (Map<String, String>) resultList.get(i);
                if (result != null) {
                    Iterator<Map.Entry<String, String>> it = result.entrySet().iterator();
                    List<T> list = new ArrayList<T>();
                    while (it.hasNext()){
                        Map.Entry<String, String> resultEntry = it.next();
                        String jsonStr = result.get(resultEntry.getKey());
                        if(StringUtils.isNotEmpty(jsonStr)){
                            T t = JSON.parseObject(jsonStr, clazz);
                            list.add(t);
                        }
                    }
                    resultMap.put(entry.getKey(), list);
                }
                i++;
            }
        }
        return resultMap;
    }
    /**
     * HINCRBY key field increment
     * Increment the integer value of a hash field by the given number
     * @param dbIndex
     * @param key
     * @param hashKey
     * @param delta
     * @return
     */
    public Long hincrby(int dbIndex, String key, Object hashKey, long delta) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().increment(key, hashKey, delta);
    }

    /**
     * HINCRBYFLOAT key field increment
     * Increment the float value of a hash field by the given amount
     * @param dbIndex
     * @param key
     * @param hashKey
     * @param delta
     * @return
     */
    public Double hincrbyfloat(int dbIndex, String key, Object hashKey, float delta) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().increment(key, hashKey, delta);
    }

    /**
     * HKEYS key
     * Get all the fields in a hash
     * @param dbIndex
     * @param key
     * @return
     */
    public Set<Object> hkeys(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().keys(key);
    }

    /**
     * HKEYS key
     * Batch get all the fields in a hash
     * @param dbIndex
     * @param map
     * @return
     */
    public Map<String, Set<String>> multihkeys(int dbIndex, Map<String, String> map) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String,  Set<String>> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                try {
                    connection.hKeys(redisTemplate.getStringSerializer().serialize(entry.getValue()));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multihgetList error", e);
                }
            }
            return null;
        });
        if (resultList != null && resultList.size() == map.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Set<String> result = (Set<String>) resultList.get(i);
                if (result != null) {
                    resultMap.put(entry.getKey(), result);
                }
                i++;
            }
        }
        return resultMap;
    }

    /**
     * HLEN key
     * Get the number of fields in a hash
     * @param dbIndex
     * @param key
     * @return
     */
    public Long hlen(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().size(key);
    }

    /**
     * HMGET key field [field ...]
     * Get the values of all the given hash fields
     * @param dbIndex
     * @param key
     * @param hashKeys
     * @return
     */
    public List<Object> hmget(int dbIndex, String key, Collection<Object> hashKeys) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().multiGet(key, hashKeys);
    }

    /**
     * HMSET key field value [field value ...]
     * Set multiple hash fields to multiple values
     * @param dbIndex
     * @param key
     * @param map
     * @return
     */
    public Boolean hmset(int dbIndex, String key, Map<Object, Object> map) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.opsForHash().putAll(key, map);
        return true;
    }

    /**
     * HSET key field value
     * Set the string value of a hash field
     * @param dbIndex
     * @param key
     * @param hashKey
     * @param t
     * @return
     */
    public <T> Boolean hset(int dbIndex, String key, Object hashKey, T t) {
    	boolean result = true;
    	try{
	        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
            String json;
            if (t != null && t instanceof String) {
                json = t.toString();
            } else {
                json = JSON.toJSONString(t);
            }
	        redisTemplate.opsForHash().put(key, hashKey, json);
    	} catch (Exception e) {
            logger.error("RedisTemplateHelper.set error, key=" + key + " value=" + t, e);
            result = false;
        }
    	return result;
    }

    /**
     * HSETNX key field value
     * Set the value of a hash field, only if the field does not exist
     * @param dbIndex
     * @param key
     * @param hashKey
     * @param value
     * @return
     */
    public Boolean hsetnx(int dbIndex, String key, Object hashKey, Object value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().putIfAbsent(key, hashKey, value);
    }

    /**
     * HVALS key
     * Get all the values in a hash
     * @param dbIndex
     * @param key
     * @return
     */
    public List<Object> hvals(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForHash().values(key);
    }

    public Map<String, Long> multihlen(int dbIndex, Map<String, String> keyMap) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, Long> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String value : keyMap.values()) {
                try {
                    connection.hLen(redisTemplate.getStringSerializer().serialize(value));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multihlen error", e);
                }
            }

            return null;
        });
        if (resultList != null && resultList.size() == keyMap.size()) {
            int i = 0;
            for (String key : keyMap.keySet()) {
                resultMap.put(key, (Long)resultList.get(i));
                i++;
            }
        }
        return resultMap;
    }

    /**
     * 批量获取多个hash的1个field
     */
    public Map<String, Object> multihget(int dbIndex, Map<String, String> keyMap, String field) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, Object> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String value : keyMap.values()) {
                try {
                    connection.hGet(redisTemplate.getStringSerializer().serialize(value), redisTemplate.getStringSerializer().serialize(field));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multihget error", e);
                }
            }

            return null;
        });
        if (resultList != null && resultList.size() == keyMap.size()) {
            int i = 0;
            for (String key : keyMap.keySet()) {
                resultMap.put(key, resultList.get(i));
                i++;
            }
        }
        return resultMap;
    }

    /**
     * 批量获取多个hash的多个field
     */
    public Map<String, Map<String, String>> multihget(int dbIndex, Map<String, String> keyMap, List<String> fieldList) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, Map<String, String>> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            //list转成byte数组
            byte[][] fields = new byte[fieldList.size()][];
            for (int  i = 0; i < fieldList.size(); i++) {
                fields[i] = redisTemplate.getStringSerializer().serialize(fieldList.get(i));
            }
            for (String value : keyMap.values()) {
                try {
                    connection.hMGet(redisTemplate.getStringSerializer().serialize(value), fields);
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multihget error", e);
                }
            }

            return null;
        });
        if (resultList != null && resultList.size() == keyMap.size()) {
            int i = 0;
            for (String key : keyMap.keySet()) {
                List<String> result = (List<String>) resultList.get(i);
                int j = 0;
                Map<String, String> map = new HashMap<>();
                for (String field : fieldList) {
                    String value = result.get(j);
                    map.put(field, value);
                    j++;
                }
                resultMap.put(key, map);
                i++;
            }
        }
        return resultMap;
    }

    public <T> Map<String, T> multihget(int dbIndex, String key, Map<String, String> map, Class<T> clazz) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, T> resultMap = new HashMap<String, T>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                try {
                    connection.hGet(redisTemplate.getStringSerializer().serialize(key), redisTemplate.getStringSerializer().serialize(entry.getValue()));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multiget error", e);
                }
            }
            return null;
        });
        if (resultList != null && resultList.size() == map.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Object result = resultList.get(i);
                if (result != null && !StringUtils.isEmpty(result.toString())) {
                    try {
                        resultMap.put(entry.getKey(), JSON.parseObject(result.toString(), clazz));
                    } catch (Exception e) {
                        logger.error("RedisTemplateHelper.multiget error", e);
                    }
                }
                i++;
            }
        }
        return resultMap;
    }

    public Boolean multihincrby(int dbIndex, String key, Map<String, Long> map) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                connection.hIncrBy(((RedisSerializer<String>) redisTemplate.getKeySerializer()).serialize(key),
                        ((RedisSerializer<String>) redisTemplate.getHashKeySerializer()).serialize(entry.getKey()), entry.getValue());
            }
            return null;
        });
        return true;
    }

    //ops for set

    /**
     * SADD key member [member ...]
     * Add one or more members to a set
     * @param dbIndex
     * @param key
     * @param values
     * @return
     */
    public Long sadd(int dbIndex, String key, String...values) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().add(key, values);
    }

    public Map<String, Long> multisadd(int dbIndex, String key, Map<String, String> valueMap) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, Long> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String value : valueMap.values()) {
                try {
                    connection.sAdd(redisTemplate.getStringSerializer().serialize(key), redisTemplate.getStringSerializer().serialize(value));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multisadd error", e);
                }
            }

            return null;
        });
        if (resultList != null && resultList.size() == valueMap.size()) {
            int i = 0;
            for (String value : valueMap.keySet()) {
                Long result = (Long) resultList.get(i);
                resultMap.put(value, result);
                i++;
            }
        }
        return resultMap;
    }

    /**
     * SCARD key
     * Get the number of members in a set
     * @param dbIndex
     * @param key
     * @return
     */
    public Long scard(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().size(key);
    }

    /**
     * SDIFF key [key ...]
     * Subtract multiple sets
     * @param dbIndex
     * @param key
     * @param otherKey
     * @return
     */
    public Set<String> sdiff(int dbIndex, String key, String otherKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().difference(key, otherKey);
    }

    /**
     * SDIFF key [key ...]
     * Subtract multiple sets
     * @param dbIndex
     * @param key
     * @param otherKeys
     * @return
     */
    public Set<String> sdiff(int dbIndex, String key, Collection<String> otherKeys) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().difference(key, otherKeys);
    }

    /**
     * SDIFFSTORE destination key [key ...]
     * Subtract multiple sets and store the resulting set in a key
     * @param dbIndex
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long sdiffstore(int dbIndex, String key, String otherKey, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().differenceAndStore(key, otherKey, destKey);
    }

    /**
     * SDIFFSTORE destination key [key ...]
     * Subtract multiple sets and store the resulting set in a key
     * @param dbIndex
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    public Long sdiffstore(int dbIndex, String key, Collection<String> otherKeys, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().differenceAndStore(key, otherKeys, destKey);
    }

    /**
     * SINTER key [key ...]
     * Intersect multiple sets
     * @param dbIndex
     * @param key
     * @param otherKey
     * @return
     */
    public Set<String> sinter(int dbIndex, String key, String otherKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().intersect(key, otherKey);
    }

    /**
     * SINTER key [key ...]
     * Intersect multiple sets
     * @param dbIndex
     * @param key
     * @param otherKeys
     * @return
     */
    public Set<String> sinter(int dbIndex, String key, Collection<String> otherKeys) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().intersect(key, otherKeys);
    }

    /**
     * SINTERSTORE destination key [key ...]
     * Intersect multiple sets and store the resulting set in a key
     * @param dbIndex
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long sinterstore(int dbIndex, String key, String otherKey, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().intersectAndStore(key, otherKey, destKey);
    }

    /**
     * SINTERSTORE destination key [key ...]
     * Intersect multiple sets and store the resulting set in a key
     * @param dbIndex
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    public Long sinterstore(int dbIndex, String key, Collection<String> otherKeys, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().intersectAndStore(key, otherKeys, destKey);
    }

    /**
     * SISMEMBER key member
     * Determine if a given value is a member of a set
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public Boolean sismember(int dbIndex, String key, Object value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().isMember(key, value);
    }

    /**
     * SMEMBERS key
     * Get all the members in a set
     * @param dbIndex
     * @param key
     * @return
     */
    public Set<String> smembers(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().members(key);
    }

    /**
     * SMOVE source destination member
     * Move a member from one set to another
     * @param dbIndex
     * @param key
     * @param value
     * @param destKey
     * @return
     */
    public Boolean smove (int dbIndex, String key, String value, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().move(key, value, destKey);
    }

    /**
     * SPOP key [count]
     * Remove and return one or multiple random members from a set
     * @param dbIndex
     * @param key
     * @return
     */
    public String spop(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().pop(key);
    }

    /**
     * SRANDMEMBER key [count]
     * Get one or multiple random members from a set
     * @param dbIndex
     * @param key
     * @return
     */
    public String srandmember(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().randomMember(key);
    }

    /**
     * SRANDMEMBER key [count]
     * Get one or multiple random members from a set
     * @param dbIndex
     * @param key
     * @param count
     * @return
     */
    public List<String> srandmember(int dbIndex, String key, long count) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().randomMembers(key, count);
    }

    /**
     * SREM key member [member ...]
     * Remove one or more members from a set
     * @param dbIndex
     * @param key
     * @param values
     * @return
     */
    public Long srem(int dbIndex, String key, Object...values) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().remove(key, values);
    }

    /**
     * SUNION key [key ...]
     * Add multiple sets
     * @param dbIndex
     * @param key
     * @param otherKey
     * @return
     */
    public Set<String> sunion(int dbIndex, String key, String otherKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().union(key, otherKey);
    }

    /**
     * SUNION key [key ...]
     * Add multiple sets
     * @param dbIndex
     * @param key
     * @param otherKeys
     * @return
     */
    public Set<String> sunion(int dbIndex, String key, Collection<String> otherKeys) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().union(key, otherKeys);
    }

    /**
     * SUNIONSTORE destination key [key ...]
     * Add multiple sets and store the resulting set in a key
     * @param dbIndex
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long sunionstore(int dbIndex, String key, String otherKey, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().unionAndStore(key, otherKey, destKey);
    }

    /**
     * SUNIONSTORE destination key [key ...]
     * Add multiple sets and store the resulting set in a key
     * @param dbIndex
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    public Long sunionstore(int dbIndex, String key, Collection<String> otherKeys, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForSet().unionAndStore(key, otherKeys, destKey);
    }

    //ops for sorted set

    /**
     * ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
     * Add one or more members to a sorted set, or update its score if it already exists
     * @param dbIndex
     * @param key
     * @param value
     * @param score
     * @return
     */
    public Boolean zadd(int dbIndex, String key, String value, double score) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().add(key, value, score);
    }

    public <T> Boolean multizadd(int dbIndex, String key, List<T> memberList, List<Double> scoreList) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            int i = 0;
            for (T member : memberList) {
                connection.zAdd(((RedisSerializer<String>) redisTemplate.getKeySerializer()).serialize(key), scoreList.get(i),
                        ((RedisSerializer<String>) redisTemplate.getValueSerializer()).serialize(JSON.toJSONString(member)));
                i++;
            }
            return null;
        });
        return true;
    }

    /**
     * ZCARD key
     * Get the number of members in a sorted set
     * @param dbIndex
     * @param key
     * @return
     */
    public Long zcard(int dbIndex, String key) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().zCard(key);
    }

    /**
     * ZCOUNT key min max
     * Count the members in a sorted set with scores within the given values
     * @param dbIndex
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long zcount(int dbIndex, String key, double min, double max) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().count(key, min, max);
    }

    /**
     * 批量获取多个sorted set的count
     */
    public Map<String, Integer> multizcount(int dbIndex, Map<String, String> map, double min, double max) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, Integer> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                try {
                    connection.zCount(redisTemplate.getStringSerializer().serialize(entry.getValue()), min, max);
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multizrank error", e);
                }
            }
            return null;
        });
        if (resultList != null && resultList.size() == map.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Object result = resultList.get(i);
                if (result != null) {
                    resultMap.put(entry.getKey(), Integer.valueOf(result.toString()));
                }
                i++;
            }
        }
        return resultMap;
    }

    /**
     * ZINCRBY key increment member
     * Increment the score of a member in a sorted set
     * @param dbIndex
     * @param key
     * @param value
     * @param delta
     * @return
     */
    public Double zincrby(int dbIndex, String key, String value, double delta) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().incrementScore(key, value, delta);
    }

    /**
     * ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * Intersect multiple sorted sets and store the resulting sorted set in a new key
     * @param dbIndex
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long zinterstore(int dbIndex, String key, String otherKey, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().intersectAndStore(key, otherKey, destKey);
    }

    /**
     * ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * Intersect multiple sorted sets and store the resulting sorted set in a new key
     * @param dbIndex
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    public Long zinterstore(int dbIndex, String key, Collection<String> otherKeys, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().intersectAndStore(key, otherKeys, destKey);
    }

    /**
     * ZRANGE key start stop [WITHSCORES]
     * Return a range of members in a sorted set, by index
     * @param dbIndex
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public Set<String> zrange(int dbIndex, String key, long start, long stop) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().range(key, start, stop);
    }

    /**
     * 批量获取多个sorted set的数据
     */
    public Map<String, Set<String>> multizrange(int dbIndex, Map<String, String> map, long start, long stop) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, Set<String>> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                try {
                    connection.zRange(redisTemplate.getStringSerializer().serialize(entry.getValue()), start, stop);
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multizrange error", e);
                }
            }
            return null;
        });
        if (resultList != null && resultList.size() == map.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Object result = resultList.get(i);
                if (result != null) {
                    resultMap.put(entry.getKey(), (Set<String>) result);
                }
                i++;
            }
        }
        return resultMap;
    }

    /**
     * ZRANGEBYLEX key min max [LIMIT offset count]
     * Return a range of members in a sorted set, by lexicographical range
     * @param dbIndex
     * @param key
     * @param range
     * @return
     */
    public Set<String> zrangebylex(int dbIndex, String key, RedisZSetCommands.Range range) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().rangeByLex(key, range);
    }

    /**
     * ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
     * Return a range of members in a sorted set, by score
     * @param dbIndex
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Set<String> zrangebysocre(int dbIndex, String key, double min, double max) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().rangeByScore(key, min, max);
    }

    /**
     * ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
     * Return a range of members in a sorted set, by score
     * @param dbIndex
     * @param key
     * @param min
     * @param max
     * @param offset
     * @param count
     * @return
     */
    public Set<String> zrangebysocre(int dbIndex, String key, double min, double max, int offset, int count) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().rangeByScore(key, min, max, offset, count);
    }

    /**
     * ZRANK key member
     * Determine the index of a member in a sorted set
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public Long zrank(int dbIndex, String key, Object value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().rank(key, value);
    }

    public Map<String, Integer> multizrank(int dbIndex, Map<String, String> valueKeyMap) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        Map<String, Integer> resultMap = new HashMap<>();
        List<Object> resultList = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<String, String> entry : valueKeyMap.entrySet()) {
                try {
                    connection.zRank(redisTemplate.getStringSerializer().serialize(entry.getValue()), redisTemplate.getStringSerializer().serialize(entry.getKey()));
                } catch (Exception e) {
                    logger.error("RedisTemplateHelper.multizrank error", e);
                }
            }
            return null;
        });
        if (resultList != null && resultList.size() == valueKeyMap.size()) {
            int i = 0;
            for (Map.Entry<String, String> entry : valueKeyMap.entrySet()) {
                Long result = (Long) resultList.get(i);
                if (result != null) {
                    try {
                        resultMap.put(entry.getKey(), result.intValue());
                    } catch (Exception e) {
                        logger.error("RedisTemplateHelper.multizrank error", e);
                    }
                }
                i++;
            }
        }
        return resultMap;
    }

    /**
     * ZREM key member [member ...]
     * Remove one or more members from a sorted set
     * @param dbIndex
     * @param key
     * @param values
     * @return
     */
    public Long zrem(int dbIndex, String key, Object...values) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().remove(key, values);
    }

    /**
     * ZREMRANGEBYRANK key start stop
     * Remove all members in a sorted set within the given indexes
     * @param dbIndex
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public Long zremrangebyrank(int dbIndex, String key, long start, long stop) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().removeRange(key, start, stop);
    }

    /**
     * ZREMRANGEBYSCORE key min max
     * Remove all members in a sorted set within the given scores
     * @param dbIndex
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long zremrangebyscore(int dbIndex, String key, double min, double max) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().removeRangeByScore(key, min, max);
    }

    /**
     * ZREVRANGE key start stop [WITHSCORES]
     * Return a range of members in a sorted set, by index, with scores ordered from high to low
     * @param dbIndex
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public Set<String> zrevrange(int dbIndex, String key, long start, long stop) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().reverseRange(key, start, stop);
    }

    /**
     * ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
     * Return a range of members in a sorted set, by score, with scores ordered from high to low
     * @param dbIndex
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Set<String> zrevrangebyscore(int dbIndex, String key, double min, double max) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().reverseRangeByScore(key, min, max);
    }

    /**
     * ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
     * Return a range of members in a sorted set, by score, with scores ordered from high to low
     * @param dbIndex
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Set<String> zrevrangebyscore(int dbIndex, String key, double min, double max, int offset, int count) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().reverseRangeByScore(key, min, max, offset, count);
    }

    /**
     * ZREVRANK key member
     * Determine the index of a member in a sorted set, with scores ordered from high to low
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public Long zrevrank(int dbIndex, String key, Object value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().reverseRank(key, value);
    }

    /**
     * ZSCORE key member
     * Get the score associated with the given member in a sorted set
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public Double zscore(int dbIndex, String key, Object value) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().score(key, value);
    }

    /**
     * ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * Add multiple sorted sets and store the resulting sorted set in a new key
     * @param dbIndex
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long zunionscore(int dbIndex, String key, String otherKey, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().unionAndStore(key, otherKey, destKey);
    }

    /**
     * ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * Add multiple sorted sets and store the resulting sorted set in a new key
     * @param dbIndex
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    public Long zunionscore(int dbIndex, String key, Collection<String> otherKeys, String destKey) {
        RedisTemplate.LOCAL_DB_INDEX.set(dbIndex);
        return redisTemplate.opsForZSet().unionAndStore(key, otherKeys, destKey);
    }

    public RedisTemplate getRedisTemplate() {
        return redisTemplate;
    }

    public void setRedisTemplate(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

}
