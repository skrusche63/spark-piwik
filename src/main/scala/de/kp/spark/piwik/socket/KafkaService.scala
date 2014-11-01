package de.kp.spark.piwik.socket
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

import java.util.UUID

import akka.actor.ActorRef
import de.kp.spark.piwik.Configuration

import de.kp.spark.piwik.stream.{ConsumerContext,BaseConsumerContextFactory,PooledConsumerContextFactory}
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}

class KafkaService(val topic:String,val actor:ActorRef) {

  val group = UUID.randomUUID().toString
  
  val (brokers,zklist) = Configuration.kafka
  val consumerPool = createConsumerPool(topic,group,zklist)

  var running:Boolean = false
  
  private def execute(bytes:Array[Byte]) {
    actor ! new String(bytes)    
  }
  
  def start() {
    running = true
    read()
  }
  
  def stop() {
    running = false
  }
  
  def read() {
    
    while (running) {
      
      val consumer = consumerPool.borrowObject()   
	  consumer.read(execute)
	
      consumerPool.returnObject(consumer)
    
    }
  
  }
  
  private def createConsumerPool(topic:String,group:String,zklist:String):GenericObjectPool[ConsumerContext] = {
    
    val ctxFactory = new BaseConsumerContextFactory(topic,group,zklist)
    val pooledProducerFactory = new PooledConsumerContextFactory(ctxFactory)
    
    val poolConfig = {
    
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      
      c
    
    }
    
    new GenericObjectPool[ConsumerContext](pooledProducerFactory, poolConfig)
  
  }

}