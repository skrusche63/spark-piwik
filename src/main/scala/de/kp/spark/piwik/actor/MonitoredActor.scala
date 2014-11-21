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

import java.util.Date
import org.apache.spark.SparkContext

import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}
import akka.routing.RoundRobinRouter

import de.kp.spark.piwik.Configuration
import de.kp.spark.piwik.model._

import de.kp.spark.piwik.cache.ActorMonitor
import de.kp.spark.piwik.context.RemoteContext

import scala.concurrent.duration.DurationInt

class MonitoredActor(@transient sc:SparkContext, name:String) extends Actor with ActorLogging {

  val (heartbeat,time) = Configuration.actor      
  val (duration,retries,workers) = Configuration.router  
  
  implicit val ec = context.dispatcher
  val scheduledTask = context.system.scheduler.schedule(DurationInt(0).second, DurationInt(1).second,self,new AliveMessage())  
 
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(duration).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }
  
  protected val ctx = new RemoteContext()
  protected val router = context.actorOf(Props(new WorkerActor(sc,ctx)).withRouter(RoundRobinRouter(workers)))

  def receive = {
    /*
     * Message sent by the scheduler to track the 'heartbeat' of this actor
     */
    case req:AliveMessage => register(name)
    
    case req:ServiceRequest => {
      
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
	  val origin = sender
      val response = ask(router, req).mapTo[ServiceResponse]
      
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! failure(req)	      
	  }
      
    }
  
    case _ => {}
    
  }
  
  override def postStop() {
    scheduledTask.cancel()
  }  
   
  def failure(req:ServiceRequest):ServiceResponse = {
    
    val uid = req.data("uid")    
    new ServiceResponse(req.service,req.task,Map("uid" -> uid),ResponseStatus.FAILURE)	
  
  }

  def register(name:String) {
      
    val now = new Date()
    val ts = now.getTime()

    ActorMonitor.add(ActorInfo(name,ts))

  }

}