package de.kp.spark.piwik.markov
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

class TransitionMatrix(numRow:Int,numCol:Int) extends DoubleMatrix(numRow,numCol) {
    	
  private var scale = 100

  def setScale(scale:Int) {
	this.scale = scale
  }

  def normalize() {	
    /*
     * Laplace correction: A row that contains at least 
     * one zero value is shift by the value of 1
     */
    (0 until numRow).foreach(row => {
	  
      val transProbs = getRow(row)
      if (transProbs.min == 0) {		
        (0 until numCol).foreach(col => table(row)(col) += 1)		        
      }

    })	
		
	/* Normalize transition support */
	(0 until numRow).foreach(row => {			
	  val rowSum = getRowSum(row)
	  (0 until numCol).foreach(col => table(row)(col) = (table(row)(col) * scale) / rowSum)		
	})
  
  }

}