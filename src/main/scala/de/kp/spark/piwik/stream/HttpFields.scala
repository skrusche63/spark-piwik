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

object HttpFields {
 
  val SITE_ID:String = "idsite"	
  /*
   * _id specifies the unique visitor identifier, and must be a 16 characters hexadecimal string. 
   * Every unique visitor must be assigned a different _id and this _id must not change after it 
   * is assigned. 
   */
  val VISITOR_ID:String = "_id"
  val PAGE_URL:String = "url"

}
