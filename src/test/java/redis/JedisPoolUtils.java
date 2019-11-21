package redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Description
 * @Author alvin
 * @Date 2019-11-21 22:09:49
 */
public class JedisPoolUtils {
    public static final String HOST = "192.168.119.133";
    public static final int PORT = 6379;


    private static volatile JedisPool  jedisPool = null;
    private JedisPoolUtils(){}

    /**
     * 获取RedisPool实例（单例）
     * @return RedisPool实例
     */
    public static JedisPool getJedisPoolInstance(){
        if(null == jedisPool){
            synchronized (JedisPoolUtils.class){
                if(null == jedisPool){
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxTotal(1000);     //最大连接数
                    poolConfig.setMaxIdle(32);        //最大空闲连接数
                    poolConfig.setMaxWaitMillis(100*1000);  //最大等待时间
                    poolConfig.setTestOnBorrow(true);     //检查连接可用性

                    jedisPool = new JedisPool(poolConfig,HOST,PORT);
                }
            }
        }
        return jedisPool;
    }


    /**
     * 从连接池获取一个Jedis实例
     * @return
     */
    public static Jedis getJedisInstance(){
        return getJedisPoolInstance().getResource();
    }


    /**
     * 将 Jedis对象归还连接池
     * @param jedisPool 连接池
     * @param jedis 连接对象
     */
    public static void release(JedisPool jedisPool, Jedis jedis){
        if (null != jedisPool){
            jedisPool.returnResourceObject(jedis);
        }
    }

}
