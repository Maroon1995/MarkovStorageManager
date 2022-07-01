package com.boke.soft.dsj.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.util.Properties

object RedisUtil {

  var jedisPool: JedisPool = _

  /**
   * 获取jedis客户端
   * @return
   */
  def getJedisClient: Jedis ={

    if (jedisPool == null) { // 如果为空，创建jedis连接池
      build()
    }
    jedisPool.getResource
  }

  def build(): Unit = {
    // 获取连接池的host和端口port
    val prop: Properties = PropertiesUtil.load("config.properties")
    val host = prop.getProperty("redis.host")
    val port = prop.getProperty("redis.port")

    // 配置连接池参数
    val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) // 忙碌是否等待
    jedisPoolConfig.setMaxWaitMillis(5000) //忙碌等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
    // 创建连接池
    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
  }

}
