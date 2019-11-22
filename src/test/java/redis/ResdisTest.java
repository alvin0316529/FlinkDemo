package redis;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.util.*;

/**
 * @Description
 * @Author alvin
 * @Date 2019-11-21 15:02:26
 */
public class ResdisTest {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.119.133",6379);
        System.out.println(jedis.ping());

        //清空数据
        //jedis.flushDB();

        //新增数据
        //jedis.set("k1","value1");

        //获取数据
        System.out.println(jedis.get("k1"));

        //判断数据是否存在
        if(jedis.exists("xx")){
            System.out.println("exists");
        }else{
            System.out.println("not exists");
        }


        //获取所有key
        Set<String> keys = jedis.keys("*");
        for (String key : keys) {
            System.out.println("key" + key);
        }

        System.out.println(jedis.type("k1"));




    }

    /**
     * redis 存储字符串
     */
    @Test
    public void testString(){
        JedisPool jedisPool = JedisPoolUtils.getJedisPoolInstance();
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();

            //添加数据
            jedis.set("name","shanghai");
            System.out.println(jedis.get("name"));

            //拼接
            jedis.append("name"," is good");
            System.out.println(jedis.get("name"));

            //删除
            jedis.del("name");
            System.out.println(jedis.get("name"));

            //设置多个键值对
            jedis.mset("name","zhangsan","age","23","sex","male");
            System.out.println(jedis.get("name") + "-" +  jedis.get("age") + "-" + jedis.get("sex") );
            jedis.incrBy("age",3);
            System.out.println(jedis.get("name") + "-" +  jedis.get("age") + "-" + jedis.get("sex") );

            //键是否存在
            if(jedis.exists("zhangsan")){
                System.out.println("zhangsan exists");
            }else{
                System.out.println("zhangsan not exits");
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
            //JedisPoolUtils.release(jedisPool,jedis);
        }


    }


    /**
     * redis操作hash
     */
    @Test
    public void testHash(){
        JedisPool jedisPool = JedisPoolUtils.getJedisPoolInstance();
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();

            Map map = new HashMap<String,String>();
            map.put("name","zhangsan");
            map.put("age","23");
            map.put("sex","male");


            //添加数据
            jedis.hmset("user1",map);

            List<String> rsmap = jedis.hmget("user1","name","age","sex");
            System.out.println(rsmap.toString());

            //删除map中的某个键值
            jedis.hdel("user1","sex");
            System.out.println(jedis.hget("user1","sex"));

            //查看某个键中值的个数
            System.out.println(jedis.hlen("user1"));
            //是否存在key为user的记录 返回true
            System.out.println(jedis.hexists("user1","name"));
            // 返回map对象中的所有key
            System.out.println(jedis.hkeys("user1"));
            // 返回map对象中的所有value
            System.out.println(jedis.hvals("user1"));


            //遍历查询数据
            Iterator<String> iter =  jedis.hkeys("user1").iterator();
            while (iter.hasNext()){
                String key = iter.next();
                System.out.println( key + " -- " + jedis.hget("user1",key));
            }



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
            //JedisPoolUtils.release(jedisPool,jedis);
        }
    }


    /**
     * redis 操作 List
     */
    @Test
    public void testList(){
        JedisPool jedisPool = JedisPoolUtils.getJedisPoolInstance();
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            //开始前，先移除所有内容
            jedis.del("list1");
            System.out.println(jedis.lrange("list1",0,-1));

            //添加数据
            jedis.lpush("list1","java");
            jedis.lpush("list1","hadoop");
            System.out.println(jedis.lrange("list1",0,-1));

            jedis.lpush("list1","storm","spark","flink");
            System.out.println(jedis.lrange("list1",0,-1));

            //获取list1中值得个数
            System.out.println(jedis.llen("list1"));

            System.out.println(jedis.rpop("list1"));


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
            //JedisPoolUtils.release(jedisPool,jedis);
        }
    }


    /**
     * redis 操作 set
     */
    @Test
    public void testSet(){
        JedisPool jedisPool =  JedisPoolUtils.getJedisPoolInstance();
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();

            //添加数据
            jedis.sadd("set1","shanghai","beijing","lanzhou","shanghai");
            System.out.println(jedis.smembers("set1"));


            //移除 set 中数据
            jedis.srem("set1","lanzhou");

            //删除set
            //jedis.del("set1");


            // 获取所有加入的value
            System.out.println(jedis.smembers("set1").size());

            // 返回集合的元素个数
            System.out.println(jedis.scard("set1"));

            //差集
            //jedis.sdiff("","");

            //并集
            //jedis.sunion("","");


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
        }

    }


    /**
     * redis 操作 zset(sorted set)
     */
    @Test
    public void testZset(){
        JedisPool jedisPool = JedisPoolUtils.getJedisPoolInstance();
        Jedis jedis  = null;

        try {
            jedis = jedisPool.getResource();

            //清空数据
            //jedis.flushDB();

            //添加数据
            Map<String,Double> map = new HashMap<String, Double>();
            map.put("shanghai",98.0);
            map.put("shanghai",92.0);
            map.put("beijing",97.0);
            map.put("hangzhou",88.0);
            map.put("nanjing",90.0);

            //jedis.zadd("zset1",map);

            //获取所有的key
            System.out.println(jedis.zrange("zset1",0,-1));

            //元素个数
            System.out.println(jedis.zcard("zset1"));

            //元素下标
            System.out.println(jedis.zscore("zset1","shanghai"));

            //范围区间内统计个数
            System.out.println(jedis.zcount("zset1",90.0,97.0));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
        }
    }





}













