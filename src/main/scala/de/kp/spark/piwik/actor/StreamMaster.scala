package de.kp.spark.piwik.actor
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

import akka.actor.{ActorLogging,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.piwik.Configuration
import de.kp.spark.piwik.context.StreamContext

import de.kp.spark.piwik.model._
import de.kp.spark.piwik.stream.Event

import akka.routing.RoundRobinRouter

import scala.concurrent.duration.DurationInt
import akka.util.Timeout.durationToTimeout

class StreamMaster extends MonitoredActor with ActorLogging {
   
   /* Load configuration for kafka */
   val (brokers,zklist) = Configuration.kafka
  
   val kafkaConfig = Map("kafka.brokers" -> brokers)
   val sc = new StreamContext(kafkaConfig)

   val router = context.actorOf(Props(new StreamActor(sc)).withRouter(RoundRobinRouter(workers)))

  def receive = {
    /*
     * Message sent by the scheduler to track the 'heartbeat' of this actor
     */
    case req:AliveMessage => register("StreamMaster")
    
    case req:Event => {
     
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
	  val origin = sender
      val response = ask(router, req).mapTo[String]
      
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! ResponseStatus.FAILURE    
	  }
      
    }
    case _ => {}
    
  }
}