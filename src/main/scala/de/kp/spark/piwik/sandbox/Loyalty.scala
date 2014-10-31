package de.kp.spark.piwik.sandbox
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

import de.kp.scala.hmm.{HmmPredictor, HmmModel, HmmTrainer}
import scala.Array.canBuildFrom

class LoyaltyModel(model:HmmModel,hStates:Map[Int,String],oStates:Map[String,Int]) {
  
  def predict(observations:Array[String]):Array[String] = {

    /* Transform String representation of observations into Integer representation */
    val intObservations = observations.map(o => oStates(o))

    /*
     * Predict the most likely sequence of hidden states for the given model and
     * observation. Set scaled to true, if log-scaled computation is to be used.
     * 
     * This requires higher computational effort but is numerically more stable 
     * for large observation sequences.
     */
    val scaled = false
    val hiddenStates = HmmPredictor.predict(model,intObservations,scaled)

    /* Transform Integer representation of hidden states into String representation */
    hiddenStates.map(h => hStates(h))
    
  }
  
}

class LoyaltyTrainer(hiddenStates:Array[String],observableStates:Array[String]) {

  /* String representation of hidden states */
  val strHStates = hiddenStates
  val numHStates = strHStates.length
  
  /* Integer representation of hidden states */
  val intHStates = hiddenStates.zip(0 until numHStates).map(kv => (kv._2,kv._1)).toMap 
  
  /* String representation of observable states */
  val strOStates = observableStates
  val numOStates = strOStates.length

  /* Integer representation of observable states */
  val intOStates = observableStates.zip(0 until numOStates).toMap 
  
    
  def train(observations:Array[String],epsilon:Double=0.0001, maxIterations:Int=1000):LoyaltyModel = {
    
    /* Transform String representation of observations into Integer representation */
    val intObservations = observations.map(o => intOStates(o))
    
    val model = HmmTrainer.trainBaumWelch(numHStates, numOStates, intObservations, epsilon, maxIterations)
    new LoyaltyModel(model,intHStates,intOStates)
    
  }

}

object Loyalty {
  
  def train(hiddenStates:Array[String],observableStates:Array[String],observations:Array[String],epsilon:Double=0.0001, maxIterations:Int=1000):LoyaltyModel = {
    
    val trainer = new LoyaltyTrainer(hiddenStates,observableStates)
    trainer.train(observations,epsilon,maxIterations)
    
  }
  
}