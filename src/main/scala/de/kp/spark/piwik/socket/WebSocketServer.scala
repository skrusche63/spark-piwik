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

import akka.actor.{ActorSystem,Props}

import de.kp.spark.piwik.Configuration
import de.kp.spark.piwik.actor.{PageCountActor,VisitorCountActor}

object WebSocketServer {

  implicit lazy val system = ActorSystem("WebSocketSystem")

  def main(args:Array[String]) {
    
    val port = Configuration.websocket
    val service = new WebSocketService(port)
    
    /*
     * Generate actors for different web socket descriptors
     */
    val pageActor = system.actorOf(Props[PageCountActor], "PageCountActor")
    service.forResource("/pagecount/ws", Some(pageActor))

    val visitorActor = system.actorOf(Props[VisitorCountActor], "VisitorCountActor")
    service.forResource("/visitorcount/ws", Some(visitorActor))
   
    service.start()
    sys.addShutdownHook({system.shutdown;service.stop})
    
  }
  
}