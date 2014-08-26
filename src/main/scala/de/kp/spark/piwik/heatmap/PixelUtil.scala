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
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}

/*
 * Class holds dimensions of a certain image; width and height
 * is relevant to project WGS84 geo coordinates onto cartesian
 * image coordinates
 */
case class Dimension(
  width:Int,
  height:Int
)
/*
 * Class specifies field positions in piped '|' textual descriptions
 * of input data
 */
case class Positions(
  lat:Int,
  lon:Int
)

object PixelUtil {

  /**
   * This method retrieves pixels from a file where each line has fields
   * delimited by '|' and where the fields at position 'latp' and 'lonp' 
   * describe latitude and longitude coordinates
   */
  def buildPixels(sc:SparkContext,input:String,clusters:Int,iterations:Int,dim:Dimension,pos:Positions):Array[(Int,Int,Int,Int,Int)] = {
    
    val positions = sc.broadcast(pos)
    val vectors = sc.textFile(input).map(line => {
      
      val parts = line.split("\\|")
      
      val lat = parts(positions.value.lat).toDouble
      val lon = parts(positions.value.lon).toDouble
      
      Vectors.dense(Array(lat,lon))
      
    })
    
    val model = KMeans.train(vectors, clusters, iterations)
    
    val centers   = sc.broadcast(model.clusterCenters)
    val dimension = sc.broadcast(dim) 
    
    vectors.map(vector => {

      val Array(lat,lon) = vector.toArray
      
      val cluster = model.predict(vector)
      val center = centers.value(cluster)
      
      val Array(clat,clon) = center.toArray
      val (cx,cy) = toXY(clat,clon,dimension.value)
      
      val (x,y) = toXY(lat,lon,dimension.value)
      
      (cluster,cx,cy,x,y)
      
    }).collect()
    
  } 
 
  private def toXY(lat:Double,lon:Double,dim:Dimension):(Int,Int) = {
    
    ((dim.width * (0.5 + lon / 360)).toInt, (dim.height * (0.5 - lat / 180)).toInt)
  
  }

}