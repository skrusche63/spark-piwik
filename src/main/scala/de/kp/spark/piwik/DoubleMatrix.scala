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

import scala.collection.mutable.ArrayBuffer

class DoubleMatrix(numRow:Int,numCol:Int) {

  protected val table:Array[Array[Double]] = Array.fill[Double](numRow,numCol)(0.0)
  
  protected var rowLabels = Array.empty[String]
  protected var colLabels = Array.empty[String]

  def setStates(rowStates:Array[String], colStates:Array[String]) {
	
    this.rowLabels = rowStates
	this.colLabels = colStates
	
  }

  def set(row:Int,col:Int,valu:Double) {
	table(row)(col) = valu
  }
	
  def get(row:Int,col:Int):Double = table(row)(col)

  def getRow(row:Int):Array[Double] = table(row)
	
  def getRow(rowLabel:String):Array[Double] = table(rowLabels.indexOf(rowLabel))    
 
  def getRowLabel(col:Int) = rowLabels(col)

  def getColLabel(col:Int) = colLabels(col)
  
  def add(row:Int,col:Int,valu:Double) {
	table(row)(col) = table(row)(col) + valu
  }

  def add(rowLabel:String,colLabel:String,valu:Double) {
	
    val (row,col) = getRowCol(rowLabel,colLabel)
	table(row)(col) += valu
	
  }
	
  def increment(row:Int,col:Int) {
	table(row)(col) = table(row)(col) + 1
  }

  def increment(rowLabel:String, colLabel:String) {
		
    val (row,col) = getRowCol(rowLabel, colLabel)
    table(row)(col) = table(row)(col) + 1

  }
	
  def getRowSum(row:Int):Double = table(row).sum

  def getColumnSum(col:Int):Double = {
		
    var sum:Double = 0
	(0 until numRow).foreach(row => sum += table(row)(col))
	
    sum
	
  }
	
  def serialize():String = {
		
    val output = ArrayBuffer.empty[String]		
    (0 until numRow).foreach(row => output += serializeRow(row))
	
    output.mkString(";")
	
  }
  
  def serializeRow(row:Int):String = table(row).mkString(",")

  def deserialize(data:String) {
    
    val rows = data.split(";")
    (0 until rows.length).foreach(row => deserializeRow(row,rows(row)))
    
  }
	
  def deserializeRow(row:Int,data:String) {
    table(row) = data.split(",").map(_.toDouble)
  }
	

  private def getRowCol(rowLabel:String,colLabel:String):(Int,Int) = {

    val row = rowLabels.indexOf(rowLabel)
    val col = colLabels.indexOf(colLabel)		

	(row,col)
	
  }

}