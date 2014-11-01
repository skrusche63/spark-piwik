package de.kp.spark.piwik.actor

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
    case req:AliveMessage => register("StreamkMaster")
    
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