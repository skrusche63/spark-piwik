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

class SequenceBuilder {

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
      val sequence = records.map(_._4).mkString(" -1 ") + " -1 -2"
      
      (cid,sequence)
      
    }).coalesce(1)
    
    val index = sc.parallelize(Range.Long(0, sequences.count, 1),sequences.partitions.size)
    val indexed = sequences.zip(index)
    
    /*
     * Build two files from the sequences:
     * 
     * 1) sid|cid
     * 2) cid|sequence
     */
    indexed.map(valu => valu._2.toString + "|" + valu._1._1).saveAsTextFile(output + "/usrs")
    indexed.map(valu => valu._2.toString + "|" + valu._1._2).saveAsTextFile(output + "/seqs")
    
  }

}