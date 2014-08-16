package de.kp.spark.piwik
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

import scala.collection.mutable.{ArrayBuffer,HashMap}

private case class Pair(ant:String,con:String)

class MarkovBuilder {

  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds

  private val SCALE = 1
  private val STATE_DEFS = Array("SL", "SE", "SG", "ML", "ME", "MG", "LL", "LE", "LG")

  /*
   * Time thresholds
   */
  private val SMALL_DATE_THRESHOLD  = 30
  private val MEDIUM_DATE_THRESHOLD = 60
  /*
   * Amount thresholds
   */
  private val LESS_AMOUNT_THRESHOLD  = 0.9
  private val EQUAL_AMOUNT_THRESHOLD = 1.1
  
  /**
   * 
   * input: ["idsite|user|idorder|timestamp|revenue_subtotal|revenue_discount"]
   * 
   */
  def buildModel(sc:SparkContext,dataset:RDD[String]):RDD[(String,TransitionMatrix)] = {
    
    /* 
     * Group all transactions by user, then sort by timestamp and create 
     * a list of timely order states from these data; finally, filter the 
     * result to remove all transactions that are described by a single state
     */
    val stateTransactions = prepare(sc,dataset).groupBy(_._1).map(
      valu => (valu._1, createState(valu._2.toList.sortBy(_._2)))
    ).filter(valu => valu._2.length >= 2)
    
    /* 
     * Extract state pairs (previous,next), compute support for each pair 
     * and build transition probability matrix; note, that probabilities
     * are calculated by normalizing the transition (pair) support
     */
    stateTransactions.map(valu => {
      
      val (cid,cstates) = valu
      
      /* Compute support for state pairs */
      val transitions = HashMap.empty[Pair,Int]
      
      (1 until cstates.length).foreach(i => {
        
        val pair = new Pair(cstates(i-1),cstates(i))
        transitions.get(pair) match {
          
          case None => transitions += pair -> 1
          case Some(count) => transitions += pair -> (count + 1)

        }
      
      })
      
      /* Setup transition matrix from pair support */  	
	  val matrix = new TransitionMatrix(STATE_DEFS.length, STATE_DEFS.length)
      
      matrix.setScale(SCALE)
      matrix.setStates(STATE_DEFS,STATE_DEFS)
      
      for ((pair,count) <- transitions) {
        matrix.add(pair.ant, pair.con, count)        
      }
      
      /* Normalize matrix and calculate probabilities */
	  matrix.normalize()

	  (cid, matrix)
	  
    })
    
  }

  private def prepare(sc:SparkContext,dataset:RDD[String]):RDD[(String,Long,Float)] = {
    
    dataset.map(line => {
     
      val parts = line.split("|")
      /*
       * Build customer identifier by combining idsite & user;
       * for further processing, we ignore idorder, as each
       * line references a single order
       */
      val cid = parts(0) + "|" + parts(1)
      (cid,parts(3).toLong,parts(4).toFloat)
      
    })
    
  }
  
  private def createState(data:List[(String,Long,Float)]):List[String] = {
    
    var first = true
    
    var prevDate:Long    = 0
    var prevAmount:Float = 0
    
    val states = ArrayBuffer.empty[String]
    for (record <- data) {
      
      if (first) {
        
        prevDate   = record._2
        prevAmount = record._3
        
        first = false
        
      } else {
        
        val date   = record._2
        val amount = record._3
        
        val dstate = time2state(date,prevDate)
        val astate = amount2state(amount,prevAmount)

        states += (dstate + astate)
        
        prevDate   = date
        prevAmount = amount
        
      }
    
    }   
   
    states.toList
    
  }

  /**
   * Amount spent compared to previous transaction
   * 
   * L : significantly less than
   * E : more or less same
   * G : significantly greater than
   * 
   */
  private def amount2state(next:Float,previous:Float):String = {
    
    if (previous < LESS_AMOUNT_THRESHOLD * next) "L"
     else if (previous < EQUAL_AMOUNT_THRESHOLD * next) "E"
     else "G"
    
  }
  
  /**   
   * This method translates a period of time, i.e. the time 
   * elapsed since last transaction into 3 discrete states:
   * 
   * S : small, M : medium, L : large
   * 
   */
  private def time2state(next:Long,previous:Long):String = {
    
    val period = (next -previous) / DAY
    
    if (period < SMALL_DATE_THRESHOLD) "S"
    else if (period < MEDIUM_DATE_THRESHOLD) "M"
    else "L"
  
  }

}