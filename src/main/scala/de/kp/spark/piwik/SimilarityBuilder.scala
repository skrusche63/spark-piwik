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
import de.kp.spark.fsm.sim.SMeasure
import de.kp.spark.piwik.markov.DoubleMatrix

object SimilarityBuilder {

  private var matrix:DoubleMatrix = null
  
  /**
   * input = ["idsite|user|idorder|timestamp|items"]
   * 
   */
  def build(sc:SparkContext,source:RDD[String],output:String) {

    val dataset = source.map(line => {
      
      val Array(idsite,user,idorder,timestamp,items) = line.split("|")
      
      val cid = idsite + "|" + user
      (cid,idorder,timestamp.toLong,items)
      
    }).groupBy(_._2)
    
    /*
     * The dataset must be repartitioned to 1 in order
     * to assign sequence numbers (sid) properly
     */
    val sequences = dataset.map(valu => {
      
      val records = valu._2.toList.sortBy(_._3)
      
      val cid = records.head._1
      /*
       * The sequence format built is compliant with the SPMF format,
       * which is a multi-purpose starting point from sequence mining 
       */
      val sequence = records.map(_._4.split(" ").map(_.toInt)).toArray
      
      (cid,sequence)
      
    })
    
    matrix = computeSimilarity(sequences)
    
  }

  /**
   * Discover top k users that are most similar to the provided one
   */
  def getTopUsers(cid:String,k:Int):Array[String] = {
    
    val row = matrix.getRow(cid).toList
    val zipped = row.zip(0 until row.length).sortBy(_._1).reverse
    
    val slice = if (k < row.length) k else row.length
    val sliced = zipped.take(slice)

    sliced.map(valu => matrix.getColLabel(valu._2)).toArray
  
  }
  
  private def computeSimilarity(source:RDD[(String,Array[Array[Int]])]):DoubleMatrix = {

    val meas = new SMeasure()
    
    val dataset = source.collect()
    
    val numRows = dataset.length    
    val rowStates = dataset.map(valu => valu._1)
    
    val matrix = new DoubleMatrix(numRows,numRows)
    matrix.setStates(rowStates, rowStates)
    
    (0 until numRows).foreach(i => {
      (0 until numRows).foreach(j => {
        
        val seq_i = dataset(i)
        val seq_j = dataset(j)
      
        val sim = meas.compute(seq_i._2, seq_j._2)
        matrix.add(seq_i._1,seq_j._1,sim)
        
      })
      
    })
    
    matrix
    
  }
}