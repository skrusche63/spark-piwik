package de.kp.spark.piwik.rest
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

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._
import spray.httpx.encoding.Gzip
import spray.httpx.marshalling.Marshaller

import spray.routing.{Directives,HttpService,RequestContext,Route}
import spray.routing.directives.EncodingDirectives
import spray.routing.directives.CachingDirectives

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.piwik.actor.{GetMaster,MetaMaster,StatusMaster,TrainMaster}
import de.kp.spark.piwik.Configuration

import de.kp.spark.piwik.model._

class RestApi(host:String,port:Int,system:ActorSystem,@transient val sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.piwik.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system
 
  val (heartbeat,time) = Configuration.actor      
  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))

  val finder = system.actorOf(Props[GetMaster], name="GetMaster")

  val monitor = system.actorOf(Props[StatusMaster], name="StatusMaster")
  val registrar = system.actorOf(Props[MetaMaster], name="MetaMaster")
  
  val trainer = system.actorOf(Props[TrainMaster], name="TrainMaster")
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  private def routes:Route = {

    path("train" / Segment) {service => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,service)
	    }
	  }
    }  ~ 
    path("get" / Segment / Segment) {(service,concept) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,service,concept)
	    }
	  }
    }  ~ 
    path("status" / Segment) {service => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,service)
	    }
	  }
    } 
  
  }
  /**
   * The PredictiveWorks. client determines whether the provided service 
   * or concept is supported; the REST API is responsible for delegating
   * the request to the respective master actors as fast as possible
   */

  private def doGet[T](ctx:RequestContext,service:String,concept:String) = doRequest(ctx,service,"get:" + concept)

  private def doTrain[T](ctx:RequestContext,segment:String) = doRequest(ctx,"outlier","train")

  private def doStatus[T](ctx:RequestContext,service:String) = doRequest(ctx,service,"status")
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String) = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master(task),request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  /**
   * This method returns the 'raw' body provided with a Http request;
   * it is e.g. used to access the meta service to register metadata
   * specifications
   */
  private def getBodyAsString(ctx:RequestContext):String = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    httpEntity.data.asString
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }
  
  private def master(task:String):ActorRef = {
    
    val req = task.split(":")(0)   
    req match {
      
      case "get"   => finder
      case "train" => trainer
      
      case "status" => monitor
      
      case _ => null
      
    }
  }

}