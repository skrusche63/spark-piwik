package de.kp.spark.piwik.stream
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

import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties

import org.apache.commons.io.Charsets

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class Event(topic:String,data:Map[String,String])

class EventDecoder(props: VerifiableProperties) extends Decoder[Event] {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def fromBytes(bytes:Array[Byte]):Event = read[Event](new String(bytes, Charsets.UTF_8))

}

class EventEncoder(props: VerifiableProperties) extends Encoder[Event] {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def toBytes(event:Event): Array[Byte] = write[Event](event).getBytes(Charsets.UTF_8)
  
}

class PView(val site:String,val visitor:String, val pageurl:String) extends Serializable {}

object PView extends Serializable {
  
  import HttpFields._
  
  def fromStream(event:Event):PView = {

    val data = event.data    
    val site = data(SITE_ID)
    
    val visitor = data(VISITOR_ID)    
    val pageurl = data(PAGE_URL)
    
    new PView(site,visitor,pageurl)
  
  }

}
