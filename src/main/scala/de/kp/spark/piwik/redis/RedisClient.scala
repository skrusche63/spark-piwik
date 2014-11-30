package de.kp.spark.piwik.redis

import redis.clients.jedis.Jedis
import de.kp.spark.piwik.Configuration

object RedisClient {

  def apply():Jedis = {

    val (host,port) = Configuration.redis
    new Jedis(host,port.toInt)
    
  }
  
}