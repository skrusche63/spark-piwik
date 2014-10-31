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

import org.apache.spark.SparkContext

import de.kp.spark.piwik.sandbox.SparkApp

import java.awt.image.BufferedImage
import java.awt.Color

import javax.imageio.ImageIO
import java.io.File

object HeatMapApp extends SparkApp {
  
  def main(args:Array[String]) {

    val Array(geodata,worldimg,heatimg,widthstr,heightstr,latstr,lonstr) = args

    val dim = new Dimension(widthstr.toInt,heightstr.toInt)
    val pos = new Positions(latstr.toInt,lonstr.toInt)
    
    val sc = createCtxLocal("HeatMapApp")    

    val pixels = PixelUtil.buildPixels(sc,geodata,10,100,dim,pos)   
    buildImage(worldimg,heatimg,pixels,dim)
    
  }

  private def buildImage(input:String, output:String, pixels:Array[(Int,Int,Int,Int,Int)],dim:Dimension) {
    
    val width  = dim.width
    val height = dim.height
    
    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
    val graphics = image.createGraphics()

    val map = ImageIO.read(new File(input))  
    graphics.drawImage(map, 0, 0, width, height, Color.WHITE, null)

    val heatmap = HeatUtil.buildPixelHeat(pixels,width,height,0.7)
    graphics.drawImage(heatmap, 0, 0, width, height, null, null)

    graphics.setColor(new Color(255, 0, 0, 40))
    for (pixel <- pixels) {
      
      val x = pixel._4
      val y = pixel._5
      
      graphics.fillOval(x - 1, y - 1, 2, 2)
  
    }
    
    ImageIO.write(image, "png", new File(output))
  
  }

}