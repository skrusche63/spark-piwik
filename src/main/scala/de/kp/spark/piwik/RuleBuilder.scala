package de.kp.spark.piwik

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.arules.TopK

class RuleBuilder {

  def buildTopKRules(sc:SparkContext,dataset:RDD[String],k:Int=10,minconf:Double=0.8) {
    
    /* Prepare dataset */
    val transactions = prepare(sc,dataset)
    
    /* Extract rules */
    val rules = TopK.extractRDDRules(sc,transactions,k,minconf)
    val jRules = TopK.rulesToJson(rules)
     
  }
  
  /**
   * input = ["idsite|user|idorder|timestamp|items"]
   */
  def prepare(sc:SparkContext, dataset:RDD[String]):RDD[(Int,Array[String])] = {

    /*
     * Reduce dataset to items and repartition to single partition 
     */
    val items = dataset.map(line => line.split("|")(4)).coalesce(1)
    
    val index = sc.parallelize(Range.Long(0, items.count, 1),items.partitions.size)
    val zip = items.zip(index) 
    
    zip.map(valu => {
      
      val (line,no) = valu
      (no.toInt, line.split(" "))
      
    })
   
  }

}