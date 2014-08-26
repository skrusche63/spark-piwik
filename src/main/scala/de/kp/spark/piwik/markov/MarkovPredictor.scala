package de.kp.spark.piwik.markov
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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.piwik.builder.TransactionBuilder

object MarkovPredictor extends MarkovBase {
  
  private var model:Map[String,TransitionMatrix] = null
  
  def load(sc:SparkContext,input:String) {
    
    model = sc.textFile(input).map(line => {

      val Array(cid,data) = line.split("|")
      
      /* Setup transition matrix from pair support */  	
	  val matrix = new TransitionMatrix(STATE_DEFS.length, STATE_DEFS.length)
      
      matrix.setScale(SCALE)
      matrix.setStates(STATE_DEFS,STATE_DEFS)

      matrix.deserialize(data)
      
      (cid,matrix)
      
    }).collect().toMap
    
  }

  /**
   * This method predicts the next purchase horizon for a certain tenant (idsite)
   * and from a set of customer transactions within a specific time window
   */
  def predict(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[(String,Long,Float)] = {
    
    val url = settings("mysql.url")
    val database = settings("mysql.db")
    
    val user = settings("mysql.user")
    val password = settings("mysql.password")

    val tb = new TransactionBuilder(url,database,user,password)
    val conversions = tb.fromLogConversion(sc, idsite, startdate, enddate)

    predict(sc,conversions)
    
  }
  
  /**
   * input: ["idsite|user|idorder|timestamp|revenue_subtotal|revenue_discount"]
   */
  def predict(sc:SparkContext,dataset:RDD[String]):RDD[(String,Long,Float)] = {
    /* 
     * Group all transactions by user, then sort by timestamp and create 
     * a list of timely order states from these data
     */
    val predictions = prepare(dataset).groupBy(_._1).map(valu => {
      
      val data = valu._2.toList.sortBy(_._2)
      
      val (cid,lastdate,lastamount) = data.last      
      val laststate = createState(data).last
            
      /* Compute index of last state from STATE_DEFS */
      val row = STATE_DEFS.indexOf(laststate)
      /* Determine most probable next state from model */
      val nextstate = model.get(cid) match {
        
        case None => ""       
        case Some(matrix) => {
          
          /* Get transition vector from matrix */
          val probs = matrix.getRow(row)
          val maxProb = probs.max
    
          val col = probs.indexOf(maxProb)
          val next = STATE_DEFS(col)
          
          next
          
        }
        
      }
      
      val (nextdate,nextamount) = (nextDate(nextstate,lastdate),nextAmount(nextstate,lastamount))
      (cid,nextdate,nextamount)
      
    })

    predictions

  }
  
  private def nextAmount(nextstate:String,lastamount:Float):Float = {
    
    if (nextstate == "") return 0
    
    lastamount * (
    
        if (nextstate.endsWith("L")) LESS_AMOUNT_HORIZON.toFloat         
        else if (nextstate.endsWith("E")) EQUAL_AMOUNT_HORIZON.toFloat    
        else LARGE_AMOUNT_HORIZON.toFloat
    
    )
    
  }

  private def nextDate(nextstate:String,lastdate:Long):Long = {

    if (nextstate == "") return -1
    
    lastdate + DAY * (
    
        if (nextstate.startsWith("S")) SMALL_DATE_HORIZON      
        else if (nextstate.startsWith("M")) MEDIUM_DATE_HORIZON   
        else LARGE_DATE_HORIZON
        
    )
    
  }

}
