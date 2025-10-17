/*
 *  Copyright 2023 by DIMS Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.odb.common.shape

import org.apache.spark.sql.catalyst.expressions.odb.ODBSimilarityFunction
import org.apache.zookeeper.KeeperException.UnimplementedException

abstract class Shape extends Serializable {
  def minDist(other: Any, queryM: Array[(Double, Int)] = null): Double
  def dist(other: Any, queryM: Array[(Double, Int)] = null): Double

  //  def expandMultiDimension(geoObj: Shape): MultiDimension
  //  def increaseCount(value: Int): Unit
  //  def increaseCount(geoObj: Shape): Unit
  //  def decreaseCount(value: Int): Unit
  //  def decreaseCount(geoObj: Shape): Unit
  //  def getCount: Int
  //  def getMemorySize: Int
  //  def setCount(newCount: Int): Unit
  //  def expandRectangle(geoObj: Shape): Shape
  //  def approxMinDist(other: Shape): Double = {
  //    throw new UnimplementedException
  //  }
  //
  def intersects(other: Shape): Boolean
  //  def serialize: String
}
