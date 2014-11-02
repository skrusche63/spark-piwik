package de.kp.spark.piwik.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-REST project
* (https://github.com/skrusche63/spark-rest).
* 
* Spark-REST is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-REST is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-REST. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.spark.SparkContext
import akka.actor.Props

import de.kp.spark.piwik.model._
import de.kp.spark.piwik.context.GetContext

class GetActor(@transient val sc:SparkContext) extends BaseActor {

  implicit val ec = context.dispatcher

  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      
      if (isValid(req)) {
      
        /*
         * We distinguish between get requests that have to be delegated to
         * Predictiworks. via the GetContext and functionality that is intrinsic
         * part of PIWIKinsight.
         */
        req.service match {
          
          case Services.RECOMMENDATION => {
               
            val actor = context.actorOf(Props(new ALSActor(sc)))

          }
          case _ => {
        
            val service = req.service
            val message = Serializer.serializeRequest(req)
            
            val response = GetContext.send(service,message).mapTo[String]
            response.onSuccess {
              case result => origin ! Serializer.deserializeResponse(result)
            }
            response.onFailure {
              case throwable => origin ! failure(req,throwable.getMessage())	 	      
	        }
            
          }
          
        }
      
      } else {
        
        val message = Messages.REQUEST_IS_NOT_SUPPORTED
        origin ! failure(req,message)
        
      }
      
    }
    
  }

}