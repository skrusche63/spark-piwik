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

class LocationBuilder extends BaseBuilder {
  /**
   * This method retrieves selected fields from the piwi_log_conversion_item table, 
   * filtered by 'idsite' and a period of time for 'server_time':
   * 
   * output = ["idsite|user|idorder|timestamp|latitude|longitude"]
   * 
   * The output may directly be used to build a location-based heatmap
   */        
  def fromLogConversion(sc:SparkContext,idsite:Int,startdate:String,enddate:String):RDD[String] = {

    val rows = readLogConversion(sc,idsite,startdate,enddate) 
    rows.map(row => {
      
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

      val lat = row("location_latitude").asInstanceOf[Float]
      val lon = row("location_longitude").asInstanceOf[Float]
      
      "" + idsite + "|" + user + "|" + idorder + "|" + timestamp + "|" + lat + "|" + lon
    
    })
    
  }

}