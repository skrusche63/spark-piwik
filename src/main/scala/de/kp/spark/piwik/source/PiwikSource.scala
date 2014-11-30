package de.kp.spark.piwik.source
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Piwik project
* (https://github.com/skrusche63/spark-arules).
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

import de.kp.spark.piwik.Configuration
import de.kp.spark.piwik.io.JdbcReader

import scala.collection.mutable.ArrayBuffer

class PiwikSource(@transient sc:SparkContext) extends Serializable {
   
  protected val (url,database,user,password) = Configuration.mysql

  private val LOG_ITEM_FIELDS = List(
      "idsite",
      "idvisitor",
      "server_time",
      "idorder",
      /*
       * idaction_xxx are references to unique entries into the piwik_log_action table, 
       * i.e. two items with the same SKU do have the same idaction_sku; the idaction_sku
       * may therefore directly be used as an item identifier
       */
      "idaction_sku",
      "price",
      "quantity",
      "deleted")

  def items(params:Map[String,Any]):RDD[String] = {
    
    /* Retrieve site, start & end date from params */
    val site = params("site").asInstanceOf[Int]
    
    val startdate = params("startdate").asInstanceOf[String]
    val enddate   = params("enddate").asInstanceOf[String]

    val sql = query(database,site.toString,startdate,enddate)
    
    val rawset = new JdbcReader(sc,site,sql).read(LOG_ITEM_FIELDS)    
    val rows = rawset.filter(row => (isDeleted(row) == false)).map(row => {
      
      val site = row("idsite").asInstanceOf[Long]
      /* Convert 'idvisitor' into a HEX String representation */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)
      /*
       * Convert server_time into universal timestamp
       */
      val server_time = row("server_time").asInstanceOf[java.sql.Timestamp]
      val timestamp = server_time.getTime()

      val group = row("idorder").asInstanceOf[String]
      val item  = row("idaction_sku").asInstanceOf[Long]
      
      (site,user,group,timestamp,item)
      
    })
    
    rows.groupBy(_._3).map(valu => {
      
      /* Group by 'group' */
      val data = valu._2.toList      
      val output = ArrayBuffer.empty[Long]
      
      val (site,user,group,timestamp,item) = data.head
      output += item
      
      data.tail.foreach(entry => output += entry._5)      
      "" + site + "|" + user + "|" + group + "|" + timestamp + "|" + output.mkString(" ")
    
    })
  
  }
   
  /**
   * A commerce item may be deleted from a certain order
   */
  private def isDeleted(row:Map[String,Any]):Boolean = row("deleted").asInstanceOf[Boolean]

  /*
   * Table: piwik_log_conversion_item
   */
  private def query(database:String,site:String,startdate:String,enddate:String) = String.format("""
    SELECT * FROM %s.piwik_log_conversion_item WHERE idsite >= %s AND idsite <= %s AND server_time > '%s' AND server_time < '%s'
    """.stripMargin, database, site, site, startdate, enddate) 

}