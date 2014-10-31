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

import de.kp.spark.piwik.model._
import de.kp.spark.piwik.context.StatusContext

class StatusActor() extends BaseActor {

  implicit val ec = context.dispatcher

  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      if (isValid(req)) {
      
        val response = StatusContext.send(req).mapTo[ServiceResponse]
        response.onSuccess {
          case result => origin ! result
        }
        response.onFailure {
          case throwable => origin ! failure(req,throwable.getMessage())	 	      
	    }
      
      } else {
        
        val message = Messages.REQUEST_IS_NOT_SUPPORTED
        origin ! failure(req,message)
        
      }
      
    }
    
  }

}