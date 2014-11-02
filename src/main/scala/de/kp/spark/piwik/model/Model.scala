package de.kp.spark.piwik.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class ActorInfo(
  name:String,
  timestamp:Long
)

case class ActorStatus(
  name:String,
  date:String,
  status:String
)

case class PageCount(url:String,count:Long)

case class VisitorCount(visitor:String,count:Int)

case class AliveMessage()

case class ActorsStatus(items:List[ActorStatus])

case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)
case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)

case class Preference(
  user:String,product:String,score:Int
)

case class Preferences(items:List[Preference])

/*
 * Service requests are mapped onto job descriptions and are stored
 * in a Redis instance
 */
case class JobDesc(
  service:String,task:String,status:String
)

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializePageCount(pagecount:PageCount):String = write(pagecount)
  def serializeVisitorCount(visitorcount:VisitorCount):String = write(visitorcount)

  /*
   * Support for serialization and deserialization of job descriptions
   */
  def serializeJob(job:JobDesc):String = write(job)

  def deserializeJob(job:String):JobDesc = read[JobDesc](job)

  def serializeActorsStatus(stati:ActorsStatus):String = write(stati)

  def serializePreferences(preferences:Preferences):String = write(preferences)

  def serializeResponse(response:ServiceResponse):String = write(response)
  def deserializeRequest(request:String):ServiceRequest = read[ServiceRequest](request)

  def deserializeResponse(response:String):ServiceResponse = read[ServiceResponse](response)
  def serializeRequest(request:ServiceRequest):String = write(request)

}

object Services {
  /*
   * PIWIKinsight. supports Association Analysis; the respective request
   * is delegated to Predictiveworks.
   */
  val ASSOCIATION:String = "association"
  /*
   * PIWIKinsight. supports Intent Recognition; the respective request is
   * delegated to Predictiveworks.
   */
  val INTENT:String = "intent"
  /*
   * Recommendation is an internal service of PIWIKinsight and uses ALS to
   * predict the most preferred item for a certain user
   */
  val RECOMMENDATION:String = "recommendation"
  /*
   * PIWIKinsight. supports Series Analysis; the respectiv request is 
   * delegated to Predictiveworks.
   */
  val SERIES:String = "series"
    
  private val services = List(ASSOCIATION,INTENT,SERIES)
  
  def isService(service:String):Boolean = services.contains(service)
  
}

object Messages {
  
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameters are missing for uid '%s'.""", uid)

  def MODEL_BUILDING_STARTED(uid:String) = String.format("""Model building started for uid '%s'.""", uid)
  
  def MODEL_DOES_NOT_EXIST(uid:String):String = String.format("""Model does not exist for uid '%s'.""", uid)

  def REQUEST_IS_NOT_SUPPORTED():String = String.format("""Unknown request.""")

  def TASK_DOES_NOT_EXIST(uid:String):String = String.format("""The task with uid '%s' does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = String.format("""The task '%s' is unknown for uid '%s'.""", task, uid)
   
}

object ResponseStatus {
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
  val DATASET:String = "dataset"
  val STARTED:String = "started"
  val FINISHED:String = "finished"
    
}