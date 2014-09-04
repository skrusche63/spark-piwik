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

import com.typesafe.config.ConfigFactory

import scala.collection.mutable.HashMap

object Configuration {

  private val path = "application.conf"
  private val config = ConfigFactory.load(path)

  private val settings = HashMap.empty[String,String]
  
  /*
   * Markov Configuration
   */
  private val markovCfg = config.getConfig("markov")

  settings += "markov.small.date.threshold" -> markovCfg.getString("small.date.threshold")
  settings += "markov.medium.date.threshold" -> markovCfg.getString("medium.date.threshold")

  settings += "markov.small.data.horizon" -> markovCfg.getString("small.data.horizon")
  settings += "markov.medium.date.horizon" -> markovCfg.getString("medium.date.horizon")
  settings += "markov.large.date.horizon" -> markovCfg.getString("large.date.horizon")

  settings += "markov.less.amount.threshold" -> markovCfg.getString("less.amount.threshold")
  settings += "markov.equal.amount.threshold" -> markovCfg.getString("equal.amount.threshold")

  settings += "markov.less.amount.horizon" -> markovCfg.getString("less.amount.horizon")
  settings += "markov.equal.amount.horizon" -> markovCfg.getString("equal.amount.horizon")
  settings += "markov.large.amount.horizon" -> markovCfg.getString("large.amount.horizon")
  
  def get = settings
  
  def mysql():(String,String,String,String) = {

   val cfg = config.getConfig("mysql")
  
   val url = cfg.getString("url")
   val db  = cfg.getString("database")
  
   val user = cfg.getString("user")
   val password = cfg.getString("password")
    
   (url,db,user,password)
   
  }
  
}