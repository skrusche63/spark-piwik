package de.kp.spark.piwik.heatmap
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

import java.awt.Color

object ColorUtil {

  val s1 = 0.25d
  val s2 = 0.50d
  val s3 = 0.75d
  val s4 = 1.0d

  private def normalize(c:Int): Int = {
      
    if (c < 0) {
        0
    
    } else if (c > 255) {
        255
    
    } else {
        c
    
    }
    
  }

  def color(weight:Double):Color = {
  
    var r: Int = 0
    var g: Int = 0
    var b: Int = 0
    
    if (weight < s1) {
        // black to blue
        r = 0
        g = 0
        b = (255 * weight / s1).toInt
      
    } else if (weight < s2) {
        // blue to green
        r = 0
        g = (255 * (weight - s1) / (s2 - s1)).toInt
        b = (255 * (s2 - weight) / (s2 - s1)).toInt
      
    } else if (weight < s3) {
        // green to yellow
        r = (255 * (s3 - weight) / (s3 - s2)).toInt
        g = 255
        b = 0
      
    } else {
        // yellow to red
        r = (255 * (weight - s3) / (s4 - s3)).toInt
        g = (255 * (s4 - weight) / (s4 - s3)).toInt
        b = 0    
    }

    new Color(normalize(r), normalize(g), normalize(b), 200)
    
  }

}