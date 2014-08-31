package de.kp.spark.piwik.arules
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

import de.kp.spark.arules.TopK

class RuleBuilder {

  /**
   * input = ["idsite|user|idorder|timestamp|items"]
   */
  def buildTopKRules(sc:SparkContext,dataset:RDD[String],k:Int=10,minconf:Double=0.8):String = {
    
    /* Prepare dataset */
    val transactions = prepare(sc,dataset)
    
    /* Extract rules and convert into JSON */
    val rules = TopK.extractRules(transactions,k,minconf)
    TopK.rulesToJson(rules)
     
  }
  
  def prepare(sc:SparkContext,dataset:RDD[String]):RDD[(Int,Array[String])] = {

    /* Reduce dataset to items and repartition to single partition */
    val items = dataset.map(line => line.split("\\|")(4)).coalesce(1)
    
    val index = sc.parallelize(Range.Long(0, items.count, 1),items.partitions.size)
    val zip = items.zip(index) 
    
    zip.map(valu => {
      
      val (line,no) = valu
      (no.toInt, line.split(" "))
      
    })
   
  }

}