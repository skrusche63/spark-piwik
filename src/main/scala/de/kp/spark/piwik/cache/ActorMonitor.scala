package de.kp.spark.piwik.cache

import java.util.Date

import de.kp.spark.piwik.Configuration
import de.kp.spark.piwik.model._

import scala.collection.mutable.ArrayBuffer

object ActorMonitor {
  
  private val (heartbeat,time) = Configuration.actor
  
  private val size = Configuration.cache
  private val cache = new LRUCache[String,ArrayBuffer[Long]](size)

  def add(info:ActorInfo) {
    
    val k = info.name
    cache.get(k) match {
      case None => {
        
        val buffer = ArrayBuffer.empty[Long]
        buffer += info.timestamp
      
        cache.put(k,buffer)
        
      }
      
      case Some(buffer) => buffer += info.timestamp
      
    }
    
    
  }
  
  def isAlive(names:Seq[String]):ActorsStatus = {
    
    val now = new Date().toString()
    val actors = ArrayBuffer.empty[ActorStatus]
    
    for (name <- names) {
      
      val status = if (isAlive(name)) "active" else "inactive"
      actors += ActorStatus(name,now,status)
      
    }
    
    ActorsStatus(actors.toList)
  
  }
  
  def isAlive(name:String):Boolean = {
    
    val alive = cache.get(name) match {
      
      case None => false
      
      case Some(buffer) => {
        
        val last = buffer.last
        val now = new Date()
        
        val ts = now.getTime()
        if (ts - last > heartbeat * 1000) false else true
        
      }
      
    }

    alive
    
  }

}