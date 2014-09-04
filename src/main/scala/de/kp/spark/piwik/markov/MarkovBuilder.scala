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

import scala.collection.mutable.HashMap

import de.kp.spark.piwik.builder.{StateBuilder,TransactionBuilder}

private case class Pair(ant:String,con:String)

object MarkovBuilder extends StateBuilder {

  /**
   * Build and persist Markov Model for a certain idsite and a period of time 
   */
  def buildModel(sc:SparkContext,idsite:Int,startdate:String,enddate:String, output:String) {
    
    /* Generate model */
    val model = buildModel(sc,idsite,startdate,enddate)
    
    /*
     * Serialize and persist model
     */
    model.map(valu => valu._1 + "|" + valu._2.serialize()).saveAsTextFile(output)

  }

  def buildModel(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[(String,TransitionMatrix)] = {

    val tb = new TransactionBuilder()
    val conversions = tb.fromLogConversion(sc, idsite, startdate, enddate)

    buildModel(sc,conversions)
    
  }

  /**
   * input: RDD["idsite|user|idorder|timestamp|revenue_subtotal|revenue_discount"]
   */
  def buildModel(sc:SparkContext,dataset:RDD[String]):RDD[(String,TransitionMatrix)] = {
    
    /* 
     * Group all transactions by user, then sort by timestamp and create 
     * a list of timely order states from these data; finally, filter the 
     * result to remove all transactions that are described by a single state
     */
    val states = prepare(dataset).groupBy(_._1).map(
      valu => (valu._1, createState(valu._2.toList.sortBy(_._2)))
    ).filter(valu => valu._2.length >= 2)
    
    /* 
     * Extract state pairs (previous,next), compute support for each pair 
     * and build transition probability matrix; note, that probabilities
     * are calculated by normalizing the transition (pair) support
     */
    states.map(valu => {
      
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

}