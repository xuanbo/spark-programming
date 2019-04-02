package com.example.spark.streaming

import java.util.Objects

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * redis client
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object InternalRedisClient extends Serializable {

  @transient
  private var pool: JedisPool = _

  def makePool(redisHost: String = "localhost",
               redisPort: Int = 6379,
               redisTimeout: Int = 3000,
               maxTotal: Int = 16,
               maxIdle: Int = 8,
               minIdle: Int = 2,
               testOnBorrow: Boolean = true,
               testOnReturn: Boolean = false,
               maxWaitMillis: Long = 10000): Unit = {
    if (pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

      sys.addShutdownHook {
        pool.destroy()
      }
    }
  }

  private def getPool: JedisPool = {
    Objects.requireNonNull(pool)
  }

  def getResource: Jedis = {
    getPool.getResource
  }

  def returnResource(jedis: Jedis): Unit = {
    if (jedis != null) {
      jedis.close()
    }
  }
}
