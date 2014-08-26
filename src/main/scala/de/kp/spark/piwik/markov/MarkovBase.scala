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

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

import de.kp.spark.piwik.Configuration

class MarkovBase extends Serializable {

  protected val DAY = 24 * 60 * 60 * 1000 // day in milliseconds

  protected val SCALE = 1
  protected val STATE_DEFS = Array("SL", "SE", "SG", "ML", "ME", "MG", "LL", "LE", "LG")

  protected val settings = Configuration.get
  /*
   * Time thresholds
   */
  protected val SMALL_DATE_THRESHOLD  = settings("markov.small.date.threshold").toInt
  protected val MEDIUM_DATE_THRESHOLD = settings("markov.medium.date.threshold").toInt
  
  protected val SMALL_DATE_HORIZON  = settings("markov.small.data.horizon").toInt
  protected val MEDIUM_DATE_HORIZON = settings("markov.medium.date.horizon").toInt
  protected val LARGE_DATE_HORIZON  = settings("markov.large.date.horizon").toInt
  
  /*
   * Amount thresholds
   */
  protected val LESS_AMOUNT_THRESHOLD  = settings("markov.less.amount.threshold").toDouble
  protected val EQUAL_AMOUNT_THRESHOLD = settings("markov.equal.amount.threshold").toDouble
  
  protected val LESS_AMOUNT_HORIZON  = settings("markov.less.amount.horizon").toDouble
  protected val EQUAL_AMOUNT_HORIZON = settings("markov.equal.amount.horizon").toDouble
  protected val LARGE_AMOUNT_HORIZON = settings("markov.large.amount.horizon").toDouble
  
  protected def prepare(dataset:RDD[String]):RDD[(String,Long,Float)] = {
    
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
  
  protected def createState(data:List[(String,Long,Float)]):List[String] = {
    
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
    
    if (next < LESS_AMOUNT_THRESHOLD * previous) "L"
     else if (next < EQUAL_AMOUNT_THRESHOLD * previous) "E"
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