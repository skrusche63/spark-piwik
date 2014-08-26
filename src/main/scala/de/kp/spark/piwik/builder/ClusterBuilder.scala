package de.kp.spark.piwik.builder

import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}

class ClusterBuilder {

   /**
   * The method clusters the transactions and determines for each transaction 
   * the respective cluster and the corresponding cluster geo coordinates
   * 
   * input = ["idsite|user|idorder|timestamp|latitude|longitude"]
   */
  def buildGeoCluster(dataset:RDD[String],clusters:Int,iterations:Int,path:String) {

    val sc = dataset.context
    val transactions = dataset.map(line => line.split("\\|"))
    
    /* Prepare dataset */
    val vectors = transactions.map(trans => {
      
      val Array(idsite,user,idorder,timestamp,latitude,longitude) = trans
      
      val lat = latitude.toFloat.toDouble
      val lon = longitude.toFloat.toDouble
      
      Vectors.dense(Array(lat,lon))
      
    })   
    
    /* Train model */
    val model = KMeans.train(vectors, clusters, iterations)
    val centers = sc.broadcast(model.clusterCenters)
    
    /* Apply model */
    transactions.map(trans => {

      val Array(idsite,user,idorder,timestamp,latitude,longitude) = trans
      
      val lat = latitude.toFloat.toDouble
      val lon = longitude.toFloat.toDouble
      
      val vector = Vectors.dense(Array(lat,lon))

      val cluster = model.predict(vector)
      val center = centers.value(cluster)
      
      val Array(clat,clon) = center.toArray

      "" + cluster + "|" + clat + "|" + clon + "|" + lat+ "|" + lon
    
    }).saveAsTextFile(path)

  }
}