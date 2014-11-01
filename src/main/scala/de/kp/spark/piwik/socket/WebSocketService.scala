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

import akka.actor.ActorRef
import java.net.InetSocketAddress

import org.java_websocket.WebSocket
import org.java_websocket.framing.CloseFrame
import org.java_websocket.server.{WebSocketServer => WSS}
import org.java_websocket.handshake.ClientHandshake

import scala.collection.mutable.Map

object WebSocketService {
  
  sealed trait WSMessage
  
  case class Message(ws:WebSocket,msg:String) extends WSMessage
  
  case class Open(ws:WebSocket,hs:ClientHandshake) extends WSMessage
  case class Close(ws:WebSocket,code:Int,reason:String,external:Boolean) extends WSMessage
  
  case class Error(ws:WebSocket,ex:Exception) extends WSMessage
  
}

class WebSocketService(val port:Int) extends WSS(new InetSocketAddress(port)) {
  
  private val services = Map[String,KafkaService]()
  
  final def forResource(descriptor:String,reactor:Option[KafkaService]) {
    reactor match {
      case Some(service) => services += ((descriptor,service))
      case None => services -= descriptor
    }
  }
  
  final override def start() {

    /* Start registered services */
    for (entry <- services) {
      entry._2.start()
    }
    
    super.start()
    
  }
  
  final override def stop() {

    /* Stop registered services */
    for (entry <- services) {
      entry._2.stop()
    }
    
    super.stop()
    
  }
  
  final override def onMessage(ws : WebSocket, msg : String) {

    if (null != ws) {
      services.get(ws.getResourceDescriptor) match {
        case Some(service) => service.actor ! WebSocketService.Message(ws, msg)
        case None => ws.close(CloseFrame.REFUSE)
      }
    
    }
  }
  
  final override def onOpen(ws : WebSocket, hs : ClientHandshake) {
  
    if (null != ws) {
      services.get(ws.getResourceDescriptor) match {
        case Some(service) => service.actor ! WebSocketService.Open(ws, hs)
        case None => ws.close(CloseFrame.REFUSE)
      }
    
    }
  }
  
  final override def onClose(ws : WebSocket, code : Int, reason : String, external : Boolean) {
    
    if (null != ws) {
      services.get(ws.getResourceDescriptor) match {
        case Some(service) => service.actor ! WebSocketService.Close(ws, code, reason, external)
        case None => ws.close(CloseFrame.REFUSE)
      }
    
    }
  }
  
  final override def onError(ws : WebSocket, ex : Exception) {
    
    if (null != ws) {
      services.get(ws.getResourceDescriptor) match {
        case Some(service) => service.actor ! WebSocketService.Error(ws, ex)
        case None => ws.close(CloseFrame.REFUSE)
      }
    
    }
    
  }
}