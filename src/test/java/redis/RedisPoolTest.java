package redis;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @Description
 * @Author alvin
 * @Date 2019-11-21 22:35:29
 */
public class RedisPoolTest {
    JedisPool jedisPool = JedisPoolUtils.getJedisPoolInstance();

    @Test
    public void test01(){
        JedisPool A = JedisPoolUtils.getJedisPoolInstance();
        JedisPool B = JedisPoolUtils.getJedisPoolInstance();

        System.out.println(A == B);
    }


    @Test
    public void test02(){
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();

            //业务逻辑
            jedis.set("k3","hello jedis");
            System.out.println(jedis.get("k3"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
        }
    }
}
