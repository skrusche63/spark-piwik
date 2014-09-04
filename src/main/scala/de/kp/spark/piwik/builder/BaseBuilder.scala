package de.kp.spark.piwik.builder
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

import de.kp.spark.piwik.MySQLConnector

class BaseBuilder extends Serializable {
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
  
  /*
   * Table: piwik_log_link_visit_action 
   */
  private val sql_logLinkVisitAction = """
    SELECT * FROM analytics.piwik_log_link_visit_action WHERE idsite >= ? AND idsite <= ? AND server_time > '$1' AND server_time < '$2'
    """.stripMargin
      
  private val LOG_LINK_VISIT_ACTION_FIELDS = List(
      "idsite",
      "idvisitor",
      "server_time",
      "idvisit",
      "idaction_url"
    )
    
  def readLogConversion(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[Map[String,Any]] = {

    val fields = LOG_FIELDS
    /*
     * Access to the log_conversion table is restricted to a time window,
     * specified by a start and end date of format yyyy-mm-dd
     */
    val query = sql_logConversion.replace("$1",startdate).replace("$2",enddate)   
    MySQLConnector.readTable(sc,idsite,query,fields)  
    
  }

  def readLogConversionItem(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[Map[String,Any]] = {
    /*
     * Configured list of database fields to be taken into account
     * with this query
     */
    val fields = LOG_ITEM_FIELDS  
    /*
     * Assign start & endtime to query statement
     */  
    val query = sql_logConversionItem.replace("$1",startdate).replace("$2",enddate)    
    MySQLConnector.readTable(sc,idsite,query,fields)  

  }
  
  def readLogLinkVisitAction(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[Map[String,Any]] = {
    
    val fields = LOG_LINK_VISIT_ACTION_FIELDS
    /*
     * Access to the log_conversion table is restricted to a time window,
     * specified by a start and end date of format yyyy-mm-dd
     */
    val query = sql_logLinkVisitAction.replace("$1",startdate).replace("$2",enddate)   
    MySQLConnector.readTable(sc,idsite,query,fields)  

  }
  
}