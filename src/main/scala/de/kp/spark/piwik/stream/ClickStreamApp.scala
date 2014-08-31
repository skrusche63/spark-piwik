package de.kp.spark.piwik.stream
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

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.streaming.kafka._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import de.kp.spark.piwik.SparkApp

import java.util.UUID

class ClickStreamApp(settings:Map[String,String]) extends SparkApp {
  
  /**
   * This clickstream evaluation actually concentrates on exact computation;
   * an alternative may to use estimators, e.g. for unique pages cardinality,
   * frequent pages or visitors and more
   */
  def run() {

    val sc = createCtxLocal("ClickStreamApp")    
    /*
     * Batch duration is the time duration spark streaming uses to collect spark RDDs; 
     * with a duration of 5 seconds, for example spark streaming collects RDDs every 5 
     * seconds, which then are gathered int RDDs    
     */
    val batch  = settings("spark.batch.duration").toInt    
    val ssc = new StreamingContext(sc, Seconds(batch))

    /* Kafka configuration */
    val kafkaConfig = Map(
      "group.id" -> settings("kafka.group"),
      
      "zookeeper.connect" -> settings("kafka.zklist"),
      "zookeeper.connection.timeout.ms" -> settings("kafka.timeout")
    
    )

    val kafkaTopics = settings("kafka.topics")
      .split(",").map((_,settings("kafka.threads").toInt)).toMap   

    /*
     * The KafkaInputDStream returns a Tuple(String,String) where only the second component
     * holds the respective message; before any further processing is initiated, we therefore
     * reduce to a DStream[String]
     */
    val stream = KafkaUtils.createStream[String,Event,StringDecoder,EventDecoder](ssc, kafkaConfig, kafkaTopics, StorageLevel.MEMORY_AND_DISK).map(_._2)
    val pageviews = stream.map(PView.fromStream(_))

    /* 
     * Compute a count of views per URL seen in each batch; a batch may be 1 second
     * or any other time period the streamining context is configured for (see above)
     */
    val pagecounts = pageviews.map(view => view.pageurl).countByValue()

    /* 
     * Return the number of visitors in the last 15 seconds and repeat this action 
     * every 2 seconds
     */
    val window = Seconds(15)
    val interval = Seconds(2)
    
    val visitorcounts = pageviews.window(window,interval).map(view => (view.visitor, 1)).groupByKey().map(v => (v._1,v._2.size))

    ssc.start()
    ssc.awaitTermination()    

  }
 
}

object ClickStreamApp {
  
  def main(args:Array[String]) {
        
    val settings = Map(

      "spark.batch.duration" -> "1",
      
      "kafka.topics"  -> "publisher",
      "kafka.threads" -> "1",
      
      "kafka.group" -> UUID.randomUUID().toString,
      "kafka.zklist" -> "127.0.0.1:2181",
      
      // in milliseconds
      "kafka.timeout" -> "10000"
      
    )
    
    (new ClickStreamApp(settings)).run()

  }
}