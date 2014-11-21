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

import org.apache.spark.SparkContext

import akka.actor.{Props}

import de.kp.spark.piwik.Configuration
import de.kp.spark.piwik.context.StreamContext

import akka.routing.RoundRobinRouter

class StreamMaster(@transient sc:SparkContext) extends MonitoredActor(sc,"StreamMaster") {
   
   /* Load configuration for kafka */
   val (brokers,zklist) = Configuration.kafka
  
   val kafkaConfig = Map("kafka.brokers" -> brokers)
   val stx = new StreamContext(kafkaConfig)

   override val router = context.actorOf(Props(new StreamActor(stx)).withRouter(RoundRobinRouter(workers)))

}