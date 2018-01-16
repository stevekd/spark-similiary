package com.bbtree;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenzhilei on 16/5/17.
 */
public class RedisTools {
    private static final Logger logger = LoggerFactory.getLogger(RedisTools.class);
    private static GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    private static List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
    private static ShardedJedisPool pool = null;

    static {
        config.setMaxTotal(1024);
        config.setMaxIdle(200);
        config.setMinIdle(10);
        config.setMaxWaitMillis(5000);
        shards.add(new JedisShardInfo("kafka01", 6379));
        shards.add(new JedisShardInfo("hz-hadoopredis-01", 6379));
        pool = new ShardedJedisPool(config, shards);
    }


    public static ShardedJedis getJedis() {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
        } catch (Exception e) {
            logger.error("getJedis错误," + e.getMessage(), e);
        }
        return jedis;
    }


    public static String getKeyCode(String uuid) {
        ShardedJedis jedis = null;
        String keyCode = "";
        try {
            jedis = RedisTools.getJedis();
            keyCode = jedis.get(uuid);
            if (keyCode == null || "".equals(keyCode.trim())) {
                keyCode = DBTools.getKeyCodeOfJDBC(uuid);
                jedis.set(uuid, keyCode);
            }
        } catch (Exception e) {
            logger.error("getKeyCode错误," + e.getMessage(), e);
        } finally {
            assert jedis != null;
            jedis.close();
        }
        return keyCode;
    }
}
