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
import java.awt.image.BufferedImage

import scala.collection.mutable.HashMap

object HeatUtil {

  /**
   * The method determines the heat for each pixel of the image with respect to 
   * the cartesian points provided and draws a colored image
   */
  def buildPixelHeat(points:Array[(Int,Int,Int,Int,Int)],width:Int,height:Int,threshold:Double):BufferedImage = {

    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
    for (x <- 0 until width; y <- 0 until height) {
      
      val heat = calculatePixelHeat(x,y,points,width,height)
      val weight = heat /threshold
      
      val color = ColorUtil.color(weight)
      image.setRGB(x, y, color.getRGB)

    }

    image
    
  }
  /**
   * The method determines the heat for each point with respect to all other cartesian
   * points provided and draws a colored image
   */
  def buildPointHeat(points:Array[(Int,Int,Int,Int,Int)], width:Int, height:Int, threshold:Double):BufferedImage = {

    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
  
    val heatmap = calculatePointHeatMap(points)
    
    for (x <- 0 until width; y <- 0 until height) {
      
      val heat = heatmap.getOrElse((x,y),0.0)
      //val heat = calculatePixelHeat(x,y,pixels,width,height)
      val weight = heat /threshold
      
      val color = ColorUtil.color(weight)
      image.setRGB(x, y, color.getRGB)

    }

    image
    
  }
  
  private def calculatePixelHeat(x:Int,y:Int, pixels:Array[(Int,Int,Int,Int,Int)],width:Int,height:Int): Double = {
    
    var heat = 0.0d
    
    val intensity = 1.0d    
    val maximium  = width / 5
    
    pixels.foreach(pixel => {

      val pixx = pixel._4
      val pixy = pixel._5
      
      if (Math.abs(pixx - x) < maximium && Math.abs(pixy - y) < maximium) {

        val distanceSquare = ((pixx - x) * (pixx - x)) + ((pixy - y) * (pixy - y))
        if (distanceSquare == 0) {
          heat += intensity
        
        } else {
          heat += intensity / distanceSquare
        }
      
      }
    
    })
    
    heat
  
  }
  
  private def calculatePointHeat(x:Int,y:Int, pixels:Array[(Int,Int,Int,Int,Int)]): Double = {
    
    var heat = 0.0d
    
    val intensity = 1.0d    
    pixels.foreach(pixel => {

      val pixx = pixel._4
      val pixy = pixel._5

      val distanceSquare = ((pixx - x) * (pixx - x)) + ((pixy - y) * (pixy - y))
      if (distanceSquare == 0) {
        heat += intensity
        
      } else {
        heat += intensity / distanceSquare
      }
    
    })
    
    heat
  
  }
   
  private def calculatePointHeatMap(pixels:Array[(Int,Int,Int,Int,Int)]):HashMap[(Int,Int),Double] = {
    
    val map = HashMap.empty[(Int,Int),Double]
    pixels.foreach(pixel => {
      
      val x = pixel._4
      val y = pixel._5
      
      map += (x,y) -> calculatePointHeat(x,y,pixels)
      
    })
    
    map
    
  } 
 
}