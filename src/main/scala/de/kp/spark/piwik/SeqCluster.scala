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
  
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.clustering.KMeans

import de.kp.spark.fsm.sim.SMeasure

private case class Sequence(sid:Int,itemsets:Array[Array[Int]])

object SeqCluster {

  def build(sc:SparkContext,path:String,clusters:Int,iterations:Int) {
    /*
     * Build similarity (engagement) vectors
     * for all the sequences provided
     */
    val file = sc.textFile(path + "/seqs")
    val source = file.map(line => {
      
      val Array(sid,sequence) = line.split("|")
      val itemsets = sequence.replace(" -2","").split("-1").map(itemset => itemset.stripMargin.split(" ").map(_.toInt))
   
      new Sequence(sid.toInt,itemsets)
      
    }).collect()
  
    val vecs = computeVectors(source)
    
    /*
     * Train model from similarity vectors
     */
    val rddvecs = sc.parallelize(vecs.map(_._2))   
    val model = KMeans.train(rddvecs, clusters, iterations)
    
    /*
     * Apply model and determine cluster identifier
     * for each similarity engagement vector
     */
    val clustered = vecs.map(vec => "" + model.predict(vec._2) + "|" + vec._1)
    sc.parallelize(clustered).saveAsTextFile(path + "/cluster")
    
  }

  private def computeVectors(sequences:Array[Sequence]):Array[(Int,Vector)] = {
    sequences.map(seq => computeVector(seq,sequences))
  }
  
  private def computeVector(seq1:Sequence,sequences:Array[Sequence]):(Int,Vector) = {
    
    val meas = new SMeasure()
    
    val sid = seq1.sid
    val vec = sequences.map(seq2 => meas.compute(seq1.itemsets, seq2.itemsets))
    
    (sid,Vectors.dense(vec))
    
  }

}