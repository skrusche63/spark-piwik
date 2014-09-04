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

import java.sql.{Connection,DriverManager,ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{JdbcRDD,RDD}

import scala.collection.mutable.HashMap

object MySQLConnector {

  private val MYSQL_DRIVER   = "com.mysql.jdbc.Driver"
  private val NUM_PARTITIONS = 1
   
  private val (url,database,user,password) = Configuration.mysql
  
  def readTable(sc:SparkContext,idsite:Int,query:String,fields:List[String]):RDD[Map[String,Any]] = {
    
    val result = new JdbcRDD(sc,() => getConnection(url,database,user,password),
      query,idsite,idsite,NUM_PARTITIONS,
      (rs:ResultSet) => getRow(rs,fields)
    ).cache()

    result
    
  }
  
  /**
   * Convert database row into Map[String,Any]
   */
  private def getRow(rs:ResultSet,fields:List[String]):Map[String,Any] = {
    
    val metadata = rs.getMetaData()
    val numCols  = metadata.getColumnCount()
    
    val row = HashMap.empty[String,Any]
    (1 to numCols).foreach(i => {
      
      val k = metadata.getColumnName(i)
      val v = rs.getObject(i)
      
      if (fields.isEmpty) {
        row += k -> v
        
      } else {        
        if (fields.contains(k)) row += k -> v
        
      }
      
    })
   
    row.toMap
    
  }
  
  private def getConnection(url:String,database:String,user:String,password:String):Connection = {

    /* Create MySQL connection */
	Class.forName(MYSQL_DRIVER).newInstance()	
	val endpoint = getEndpoint(url,database)
		
	/* Generate database connection */
	val	connection = DriverManager.getConnection(endpoint,user,password)
    connection
    
  }
  
  private def getEndpoint(url:String,database:String):String = {
		
	val endpoint = "jdbc:mysql://" + url + "/" + database
	endpoint
		
  }

}