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

import java.nio.charset.Charset

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.commons.codec.binary.Hex

import scala.collection.mutable.ArrayBuffer

/**
 * The TransactionBuilder is the bridge between the visitor engagement data stored
 * in a MySQL database and more sophisticate mining & prediction techniques
 */
class TransactionBuilder(url:String,database:String,user:String,password:String) extends Serializable {

  /*
   * Table: piwik_log_conversion
   */
  private val sql_logConversion = """
    SELECT * FROM analytics.piwik_log_conversion WHERE idsite >= ? AND idsite <= ? AND server_time > '$1' AND server_time < '$2'
    """.stripMargin 

  private val LOG_FIELDS = List(
      "idsite",
      "idvisitor",
      "server_time",
      "location_country",
      "location_region",
      "location_city",
      "location_latitude",
      "location_longitude",
      "idgoal",
      "idorder",
      "items",
      "revenue",
      "revenue_subtotal",
      "revenue_tax",
      "revenue_shipping",
      "revenue_discount")

  /*
   * Table: piwik_log_conversion_item
   */
  private val sql_logConversionItem = """
    SELECT * FROM analytics.piwik_log_conversion_item WHERE idsite >= ? AND idsite <= ? AND server_time > '$1' AND server_time < '$2'
    """.stripMargin 

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
    
  /**
   * This method retrieves selected fields from the piwi_log_conversion_item table, filtered
   * by 'idsite' and a period of time for 'server_time':
   * 
   * output = ["idsite|user|idorder|timestamp|revenue_subtotal|revenue_discount"]
   * 
   * The output may directly be used to build markov states and predict e.g. the purchase horizon
   */        
  def fromLogConversion(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[String] = {

    val fields = LOG_FIELDS
    /*
     * Access to the log_conversion table is restricted to a time window,
     * specified by a start and end date of format yyyy-mm-dd
     */
    val query = sql_logConversion.replace("$1",startdate).replace("$2",enddate)   
    val rows = MySQLConnector.readTable(sc,url,database,user,password,idsite,query,fields)  
  
    /*
     * Restrict to conversion that refer to ecommerce orders (idgoal = 0)
     */
    rows.filter(row => isOrder(row)).map(row => {
      
      val idsite  = row("idsite").asInstanceOf[Long]
      /*
       * Convert 'idvisitor' into a HEX String representation
       */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)
      /*
       * Convert server_time into universal timestamp
       */
      val server_time = row("server_time").asInstanceOf[java.sql.Timestamp]
      val timestamp = server_time.getTime()
      
      val idorder = row("idorder").asInstanceOf[String]      
      val items = row("items").asInstanceOf[Int]
      
      /*
       * For further analysis it is actually sufficient to
       * focus on revenue_subtotal and revenue_discount
       */
      val revenue_subtotal = row("revenue_subtotal").asInstanceOf[Float]
      val revenue_discount = row("revenue_discount").asInstanceOf[Float]

      "" + idsite + "|" + user + "|" + idorder + "|" + timestamp + "|" + revenue_subtotal + "|" + revenue_discount
      
    })
    
  }
  /**
   * This method retrieves selected fields from the piwi_log_conversion_item table, filtered
   * by 'idsite' and a period of time for 'server_time':
   * 
   * output = ["idsite|user|idorder|timestamp|items"]
   * 
   * This output may directly be used to retrieve top K association rules
   */
  def fromLogConversionItem(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[String] = {
    /*
     * Configured list of database fields to be taken into account
     * with this query
     */
    val fields = LOG_ITEM_FIELDS  
    /*
     * Assign start & endtime to query statement
     */  
    val query = sql_logConversionItem.replace("$1",startdate).replace("$2",enddate)    
    val rows = MySQLConnector.readTable(sc,url,database,user,password,idsite,query,fields)  
    
    /*
     * Restrict to those items that are NOT deleted from the respective order
     */
    val items = rows.filter(row => (isDeleted(row) == false)).map(row => {
      
      val idsite  = row("idsite").asInstanceOf[Long]
      /*
       * Convert 'idvisitor' into a HEX String representation
       */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)
      /*
       * Convert server_time into universal timestamp
       */
      val server_time = row("server_time").asInstanceOf[java.sql.Timestamp]
      val timestamp = server_time.getTime()
      
      val idorder = row("idorder").asInstanceOf[String]      
      val idaction_sku = row("idaction_sku").asInstanceOf[Long]
    
      (idsite,user,idorder,idaction_sku,timestamp)
    
    })
    /*
     * Group items by 'idorder' and aggregate all items of a single order
     * into a single line
     */
    items.groupBy(_._3).map(valu => {
      /*
       * Sort grouped orders by (ascending) timestamp
       */
      val data = valu._2.toList.sortBy(_._5)      
      val output = ArrayBuffer.empty[String]
      
      val (idsite,user,idorder,idaction_sku,timestamp) = data.head
      output += idaction_sku.toString
      
      for (record <- data.tail) {
        output += record._4.toString
      }
      
      "" + idsite + "|" + user + "|" + idorder + "|" + timestamp + "|" + output.mkString(" ")
      
    })
    
  }
  
  /**
   * A commerce item may be deleted from a certain order
   */
  private def isDeleted(row:Map[String,Any]):Boolean = {
    
    val deleted = row("deleted").asInstanceOf[Boolean]
    deleted
    
  }
  
  /**
   * A conversion entry is specified as an ecomerce order, if the idgoal value is '0'
   */
  private def isOrder(row:Map[String,Any]):Boolean = {
    
    val idgoal = row("idgoal").asInstanceOf[Int]
    (idgoal == 0)
    
  }
}