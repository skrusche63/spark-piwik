package de.kp.spark.piwik.redis
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
 * 
 * This file is part of the Spark-Piwik project
 * (https://github.com/skrusche63/spark-piwik).
 * 
 * Spark-Piwik is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * Spark-Piwik is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with
 * Spark-Piwik. 
 * 
 * If not, see <http://www.gnu.org/licenses/>.
 */

import de.kp.spark.piwik.model._

import java.util.Date
import scala.collection.JavaConversions._

object RedisCache {

  val client  = RedisClient()
  val service = "piwik"
  
  def addModel(uid:String,model:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "model:" + service + ":" + uid
    val v = "" + timestamp + ":" + model
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addStatus(req:ServiceRequest,status:String) {
   
    val (uid,task) = (req.data("uid"),req.task)
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "job:" + service + ":" + uid
    val v = "" + timestamp + ":" + Serializer.serializeJob(JobDesc(service,task,status))
    
    client.zadd(k,timestamp,v)
    
  }
  
  def metaExists(uid:String):Boolean = {

    val k = "meta:" + uid
    client.exists(k)
    
  }
  
  def modelExists(uid:String):Boolean = {

    val k = "model:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def taskExists(uid:String):Boolean = {

    val k = "job:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def meta(uid:String):String = {

    val k = "meta:" + uid
    val metas = client.zrange(k, 0, -1)

    if (metas.size() == 0) {
      null
    
    } else {
      
      metas.toList.last
      
    }

  }
  
  def model(uid:String):String = {

    val k = "model:" + service + ":" + uid
    val models = client.zrange(k, 0, -1)

    if (models.size() == 0) {
      null
    
    } else {
      
      val last = models.toList.last
      last.split(":")(1)
      
    }
  
  }
  
  /**
   * Get timestamp when job with 'uid' started
   */
  def starttime(uid:String):Long = {
    
    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      0
    
    } else {
      
      val first = jobs.iterator().next()
      first.split(":")(0).toLong
      
    }
     
  }
  
  def status(uid:String):String = {

    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      null
    
    } else {
      
      val job = Serializer.deserializeJob(jobs.toList.last)
      job.status
      
    }

  }

}