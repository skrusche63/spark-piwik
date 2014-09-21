package de.kp.spark.piwik.recom
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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}

/**
 * 
 * Business Case
 * 
 * 1) We take users and their transactions into account, that purchased at least
 *    one item within a certain period of time. This selection determines the user
 *    and also the item basis.
 *    
 * 2) For this user and item base implicit preferences are derived and scored from
 *    1..5.   
 * 
 * 3) All recommendations are made from this user, product and preference base.
 * 
 * 4) Available output: 
 * 
 *    - Given a (user,item) pair determine rating
 * 
 *    - Given a user determine K product ratings
 *    
 *    - Given a user & list of products determine ratings
 *    
 */

class RecommenderModel(@transient val sc:SparkContext,val userspec:Dict,val itemspec:Dict,val matrix:MatrixFactorizationModel) {
  
  /**
   * Predict the preferences of a certain user for a list of products.
   */
  def predict(user:String,products:List[String]):Array[Preference] = {

    val uid = userspec.getIndex(user)
    val candidates = sc.parallelize(products.map(product => itemspec.getIndex(product)))
    
    val ratings = matrix.predict(candidates.map((uid, _))).collect
    ratings.sortBy(-_.rating).map(toPreference)
    
  }
  
  def predict(pairs:List[(String,String)]):Array[Preference] = {
    
    val data = sc.parallelize(pairs.map(pair => {
      
      val uid = userspec.getIndex(pair._1)
      val pid = itemspec.getIndex(pair._2)
      
      (uid,pid)
      
    }))

    val ratings = matrix.predict(data).collect
    ratings.sortBy(-_.rating).map(toPreference)
    
  }
  
  def recommendProducts(user:String,num:Int):Array[Preference] = {
    
    /*
     * Convert user into uid
     */
    val uid = userspec.getIndex(user)
    
    /*
     * Recommend products
     */
    val ratings = matrix.recommendProducts(uid, num)
    ratings.sortBy(-_.rating).map(toPreference)
    
  }

  private def toPreference(rating:Rating) = {
    
    Preference(userspec.getTerm(rating.user),
      itemspec.getTerm(rating.product),
      rating.rating.toInt
    )
  
  }
  
}


/**
 * Input: idsite + "|" + user + "|" + idorder + "|" + timestamp + "|" + item1 item 2 item 3
 * 
 */  
class Recommender extends Serializable {
  /*
   * We restrict to users within a certain interval
   * of ratings; the default parameters filter users
   * with ratings outside of [10..20]
   */
  val minprefs = 10
  val maxprefs = 20

  /*
   * Partitions used to partition the training dataset 
   */
  val partitions = 20

  def train(dataset:RDD[String]):RecommenderModel = {

    val sc = dataset.context
    val prefs = NPrefBuilder.build(dataset)
    /*
     * Restrict to user that match the interval criteria defined above
     */
    val trainprefs = prefs.groupBy(_.user)
                       .filter(r => minprefs <= r._2.size  && r._2.size < maxprefs)
                       .flatMap(_._2)
                       .repartition(partitions)
                       .cache()

    /*
     * Create user and product dictionaries as the ALS predictor requires 
     * integers to uniquely specify users and products
     */
    val users    = new Dict(trainprefs.map(_.user).distinct.collect)
    val products = new Dict(trainprefs.map(_.product).distinct.collect)
                   
    val bcusers = sc.broadcast(users)
    val bcproducts = sc.broadcast(products)
    
    /* Convert preferences to Spark ratings */
    val ratings = trainprefs.map(pref => {

      val uid = bcusers.value.getIndex(pref.user)
      val pid = bcproducts.value.getIndex(pref.product)
      
      Rating(uid,pid,pref.score)
      
    })
    
    /* Build model */
    val model = ALS.train((ratings).repartition(partitions), 10, 20, 0.01)
         
    new RecommenderModel(sc,users,products,model)
  
  }

}