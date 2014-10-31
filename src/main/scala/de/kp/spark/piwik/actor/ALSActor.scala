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
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.piwik.Configuration

import de.kp.spark.piwik.model._
import de.kp.spark.piwik.redis.RedisCache

import de.kp.spark.piwik.hadoop.HadoopIO

import de.kp.spark.piwik.recom.{Recommender,RecommenderModel}
import de.kp.spark.piwik.source.TransactionSource

class ALSActor(@transient val sc:SparkContext) extends BaseActor {
  
  private val base = Configuration.recommendation  
 
  def receive = {

    case req:ServiceRequest => {
      
      val uid = req.data("uid")
      req. task.split(":")(0) match {
        
        case "get" => {

          val resp = if (RedisCache.modelExists(uid) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {    

             val prediction = new RecommenderModel(sc,uid,req.data).predict()
                
             val data = Map("uid" -> uid, "recommendation" -> prediction)
             new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
          
          }
           
          sender ! Serializer.serializeResponse(resp)
          context.stop(self)
        
        }
        
        case "status" => {

          val resp = if (RedisCache.taskExists(uid) == false) {           
            failure(req,Messages.TASK_DOES_NOT_EXIST(uid))           
            
          } else {   
            status(req)
            
          }
           
          sender ! Serializer.serializeResponse(resp)
          context.stop(self)

        }
        
        case "train" => {
          /* 
           * Recommendation has no mandatory parameters; in case of no parameters
           * provided, default variables are used
           */
          val missing = false
          /* Send response to originator of request */
          sender ! response(req, missing)

          if (missing == false) {
            /* Register status */
            RedisCache.addStatus(req,ResponseStatus.STARTED)
 
            try {
          
              val source = new TransactionSource(sc)
              val items = source.get(req.data)
          
              RedisCache.addStatus(req,ResponseStatus.DATASET)
         
              val (users,products,model) = new Recommender().train(items,req.data)
          
              /* Register model */
              val now = new Date()
              val dir = base + "/als-" + now.getTime().toString
    
              HadoopIO.writeRecom(users,products,model,dir)
              /* Put model to cache */
              RedisCache.addModel(uid,dir)
          
              /* Update cache */
              RedisCache.addStatus(req,ResponseStatus.FINISHED)
          
            } catch {
              case e:Exception => RedisCache.addStatus(req,ResponseStatus.FAILURE)          
            }

          }
      
          context.stop(self)
          
        }
        
        case _ => {
           
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
          sender ! Serializer.serializeResponse(failure(req,msg))
          context.stop(self)
           
        }
        
      }
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
  
  }
 
  private def status(req:ServiceRequest):ServiceResponse = {
    
    val uid = req.data("uid")
    val data = Map("uid" -> uid)
                
    new ServiceResponse(req.service,req.task,data,RedisCache.status(uid))	

  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.MODEL_BUILDING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.STARTED)	
  
    }

  }

}