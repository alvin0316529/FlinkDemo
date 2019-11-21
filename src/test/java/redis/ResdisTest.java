package redis;

import redis.clients.jedis.Jedis;

import java.util.Set;

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
}
