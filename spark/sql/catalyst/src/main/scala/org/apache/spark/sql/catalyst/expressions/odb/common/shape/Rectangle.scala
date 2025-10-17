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

case class Rectangle(low: Point[Any], high: Point[Any]) extends Shape {
  //  require(low.coord.length == high.coord.length)
  //  require(low <= high, s"$low, $high")

  //  def this(low_x: Double, low_y: Double, high_x: Double, high_y: Double) {
  //    this(Point[Any](Array(low_x, low_y)), Point[Any](Array(high_x, high_y)))
  //  }

  val centroid = Point[Any](low.coord.asInstanceOf[Array[Double]]
    .zip(high.coord.asInstanceOf[Array[Double]]).map(x => x._1 + x._2 / 2.0), low.metricIndex, low.metricValue)

  def expandToInclude(shape: Shape): Rectangle = {
    shape match {
      case rect: Rectangle =>
        val newLow = low.coord.asInstanceOf[Array[Double]].zip(rect.low.coord.asInstanceOf[Array[Double]]).map { case (a, b) => Math.min(a, b) }
        val newHigh = high.coord.asInstanceOf[Array[Double]].zip(rect.high.coord.asInstanceOf[Array[Double]]).map { case (a, b) => Math.max(a, b) }
        Rectangle(Point(newLow, low.metricIndex, low.metricValue), Point(newHigh, high.metricIndex, high.metricValue))
      case point: Point[_] =>
        val newLow = low.coord.asInstanceOf[Array[Double]].zip(point.coord.asInstanceOf[Array[Double]]).map { case (a, b) => Math.min(a, b) }
        val newHigh = high.coord.asInstanceOf[Array[Double]].zip(point.coord.asInstanceOf[Array[Double]]).map { case (a, b) => Math.max(a, b) }
        Rectangle(Point(newLow, low.metricIndex, low.metricValue), Point(newHigh, high.metricIndex, high.metricValue))
      case _ => this
    }
  }

  def shrinkIfNecessary(shape: Shape): Rectangle = {
    shape match {
      case point: Point[_] =>
        val newLow = low.coord.asInstanceOf[Array[Double]].zip(point.coord.asInstanceOf[Array[Double]]).map {
          case (currentLow, pointCoord) =>
            if (pointCoord == currentLow) Double.PositiveInfinity else currentLow
        }
        val newHigh = high.coord.asInstanceOf[Array[Double]].zip(point.coord.asInstanceOf[Array[Double]]).map {
          case (currentHigh, pointCoord) =>
            if (pointCoord == currentHigh) Double.NegativeInfinity else currentHigh
        }

        // Validate the new bounds to ensure they form a valid rectangle
        val isValid = newLow.zip(newHigh).forall { case (l, h) => l <= h }

        if (isValid) {
          Rectangle(
            Point(newLow.map {
              case Double.PositiveInfinity => low.coord.asInstanceOf[Array[Double]].min
              case coord => coord
            }, low.metricIndex, low.metricValue),
            Point(newHigh.map {
              case Double.NegativeInfinity => high.coord.asInstanceOf[Array[Double]].max
              case coord => coord
            }, high.metricIndex, high.metricValue)
          )
        } else {
          this // If shrinking makes the rectangle invalid, return the original rectangle
        }

      case rect: Rectangle =>
        // Handle shrinking based on a rectangle
        val newLow = low.coord.asInstanceOf[Array[Double]].zip(rect.low.coord.asInstanceOf[Array[Double]]).map {
          case (currentLow, rectLow) =>
            if (currentLow == rectLow) Double.PositiveInfinity else currentLow
        }
        val newHigh = high.coord.asInstanceOf[Array[Double]].zip(rect.high.coord.asInstanceOf[Array[Double]]).map {
          case (currentHigh, rectHigh) =>
            if (currentHigh == rectHigh) Double.NegativeInfinity else currentHigh
        }

        // Validate the new bounds to ensure they form a valid rectangle
        val isValid = newLow.zip(newHigh).forall { case (l, h) => l <= h }

        if (isValid) {
          Rectangle(
            Point(newLow.map {
              case Double.PositiveInfinity => low.coord.asInstanceOf[Array[Double]].min
              case coord => coord
            }, low.metricIndex, low.metricValue),
            Point(newHigh.map {
              case Double.NegativeInfinity => high.coord.asInstanceOf[Array[Double]].max
              case coord => coord
            }, high.metricIndex, high.metricValue)
          )
        } else {
          this // If shrinking makes the rectangle invalid, return the original rectangle
        }

      case _ => this // If the shape is not a point or rectangle, return the original rectangle
    }
  }

  def intersects(other: Shape): Boolean = {
    other match {
      case p: Point[Any] => contains(p)
      case mbr: Rectangle => intersects(mbr)
      case r: SimpleRange => r.intersects(this)
    }
  }

  def intersects(other: Rectangle): Boolean = {
    require(low.coord.asInstanceOf[Array[Double]].length == other.low.coord.asInstanceOf[Array[Double]].length)
    for (i <- low.coord.asInstanceOf[Array[Double]].indices)
      if (low.coord.asInstanceOf[Array[Double]](i) > other.high.coord.asInstanceOf[Array[Double]](i) || high.coord.asInstanceOf[Array[Double]](i) < other.low.coord.asInstanceOf[Array[Double]](i)) {
        return false
      }
    true
  }

  def contains(p: Point[Any]): Boolean = {
    require(low.coord.asInstanceOf[Array[Double]].length == p.coord.asInstanceOf[Array[Double]].length)
    for (i <- p.coord.asInstanceOf[Array[Double]].indices)
      if (low.coord.asInstanceOf[Array[Double]](i) > p.coord.asInstanceOf[Array[Double]](i) || high.coord.asInstanceOf[Array[Double]](i) < p.coord.asInstanceOf[Array[Double]](i)) {
        return false
      }
    true
  }

  def contains(rectangle: Rectangle): Boolean = {
    require(low.coord.asInstanceOf[Array[Double]].length == rectangle.low.coord.asInstanceOf[Array[Double]].length)
    for (i <- low.coord.asInstanceOf[Array[Double]].indices) {
      if (low.coord.asInstanceOf[Array[Double]](i) > rectangle.low.coord.asInstanceOf[Array[Double]](i)
        || high.coord.asInstanceOf[Array[Double]](i) < rectangle.high.coord.asInstanceOf[Array[Double]](i)) {
        return false
      }
    }
    true
  }

  override def minDist(other: Any, queryM: Array[(Double, Int)] = null): Double = {
    other match {
      case p: Point[Any] => minDistPoint(p, queryM)
      case mbr: Rectangle => minDistRect(mbr, queryM)
      case r: SimpleRange => r.minDist(this)
    }
  }

  def minDistPoint(p: Point[Any], queryM: Array[(Double, Int)] = null): Double = {

    require(low.coord.asInstanceOf[Array[Double]].length == p.coord.asInstanceOf[Array[Double]].length)
    // QT: can we just use the L2 distance? and mValue * (p.coord(i) - low.coord(i)) * (p.coord(i) - low.coord(i))
    var ans = 0.0
    if (queryM != null) {
      queryM.foreach {
        case (mValue, mIndex) =>
          if (p.coord.asInstanceOf[Array[Double]](mIndex) < low.coord.asInstanceOf[Array[Double]](mIndex)) {
            ans += (low.coord.asInstanceOf[Array[Double]](mIndex) - p.coord.asInstanceOf[Array[Double]](mIndex)) * (
              low.coord.asInstanceOf[Array[Double]](mIndex) - p.coord.asInstanceOf[Array[Double]](mIndex))
          } else if (p.coord.asInstanceOf[Array[Double]](mIndex) > high.coord.asInstanceOf[Array[Double]](mIndex)) {
            ans += (p.coord.asInstanceOf[Array[Double]](mIndex) - high.coord.asInstanceOf[Array[Double]](mIndex)) * (
              p.coord.asInstanceOf[Array[Double]](mIndex) - high.coord.asInstanceOf[Array[Double]](mIndex))
          }
      }
    }
    else {
      for (i <- p.coord.asInstanceOf[Array[Double]].indices) {
        if (p.coord.asInstanceOf[Array[Double]](i) < low.coord.asInstanceOf[Array[Double]](i)) {
          ans += (low.coord.asInstanceOf[Array[Double]](i) - p.coord.asInstanceOf[Array[Double]](i)) * (low.coord.asInstanceOf[Array[Double]](i) - p.coord.asInstanceOf[Array[Double]](i))
        } else if (p.coord.asInstanceOf[Array[Double]](i) > high.coord.asInstanceOf[Array[Double]](i)) {
          ans += (p.coord.asInstanceOf[Array[Double]](i) - high.coord.asInstanceOf[Array[Double]](i)) * (p.coord.asInstanceOf[Array[Double]](i) - high.coord.asInstanceOf[Array[Double]](i))
        }
      }
    }
    Math.sqrt(ans)
  }

  def minDistRect(other: Rectangle, queryM: Array[(Double, Int)] = null): Double = {
    require(low.coord.asInstanceOf[Array[Double]].length == other.low.coord.asInstanceOf[Array[Double]].length)
    var ans = 0.0
    if (queryM != null) {
      queryM.foreach {
        case (mValue, mIndex) =>
          var x = 0.0
          if (other.high.coord.asInstanceOf[Array[Double]](mIndex) < low.coord.asInstanceOf[Array[Double]](mIndex)) {
            x = Math.abs(other.high.coord.asInstanceOf[Array[Double]](mIndex) - low.coord.asInstanceOf[Array[Double]](mIndex))
          } else if (high.coord.asInstanceOf[Array[Double]](mIndex) < other.low.coord.asInstanceOf[Array[Double]](mIndex)) {
            x = Math.abs(other.low.coord.asInstanceOf[Array[Double]](mIndex) - high.coord.asInstanceOf[Array[Double]](mIndex))
          }
          ans += x * x
      }
    } else {
      for (i <- low.coord.asInstanceOf[Array[Double]].indices) {
        var x = 0.0
        if (other.high.coord.asInstanceOf[Array[Double]](i) < low.coord.asInstanceOf[Array[Double]](i)) {
          x = Math.abs(other.high.coord.asInstanceOf[Array[Double]](i) - low.coord.asInstanceOf[Array[Double]](i))
        } else if (high.coord.asInstanceOf[Array[Double]](i) < other.low.coord.asInstanceOf[Array[Double]](i)) {
          x = Math.abs(other.low.coord.asInstanceOf[Array[Double]](i) - high.coord.asInstanceOf[Array[Double]](i))
        }
        ans += x * x
      }
    }
    Math.sqrt(ans)
  }

  override def dist(other: Any, queryM: Array[(Double, Int)] = null): Double = {
    other match {
      case p: Point[Any] => distPoint(p, queryM)
      case mbr: Rectangle => distRect(mbr, queryM)
      case r: SimpleRange => r.minDist(this)
    }
  }

  def distPoint(p: Point[Any], queryM: Array[(Double, Int)] = null): Double = {

    require(low.coord.asInstanceOf[Array[Double]].length == p.coord.asInstanceOf[Array[Double]].length)
    // QT: can we just use the L2 distance? and mValue * (p.coord(i) - low.coord(i)) * (p.coord(i) - low.coord(i))
    var ans = 0.0
    if (queryM != null) {
      queryM.foreach {
        case (mValue, mIndex) =>
          if (p.coord.asInstanceOf[Array[Double]](mIndex) < low.coord.asInstanceOf[Array[Double]](mIndex)) {
            ans += (low.coord.asInstanceOf[Array[Double]](mIndex) - p.coord.asInstanceOf[Array[Double]](mIndex)) * mValue
          } else if (p.coord.asInstanceOf[Array[Double]](mIndex) > high.coord.asInstanceOf[Array[Double]](mIndex)) {
            ans += (p.coord.asInstanceOf[Array[Double]](mIndex) - high.coord.asInstanceOf[Array[Double]](mIndex)) * mValue
          }
      }
    }
    else {
      for (i <- p.coord.asInstanceOf[Array[Double]].indices) {
        if (p.coord.asInstanceOf[Array[Double]](i) < low.coord.asInstanceOf[Array[Double]](i)) {
          ans += low.coord.asInstanceOf[Array[Double]](i) - p.coord.asInstanceOf[Array[Double]](i)
        } else if (p.coord.asInstanceOf[Array[Double]](i) > high.coord.asInstanceOf[Array[Double]](i)) {
          ans += p.coord.asInstanceOf[Array[Double]](i) - high.coord.asInstanceOf[Array[Double]](i)
        }
      }
    }
    ans
  }

  def distRect(other: Rectangle, queryM: Array[(Double, Int)] = null): Double = {
    require(low.coord.asInstanceOf[Array[Double]].length == other.low.coord.asInstanceOf[Array[Double]].length)
    var ans = 0.0
    if (queryM != null) {
      queryM.foreach {
        case (mValue, mIndex) =>
          var x = 0.0
          if (other.high.coord.asInstanceOf[Array[Double]](mIndex) < low.coord.asInstanceOf[Array[Double]](mIndex)) {
            x = Math.abs(other.high.coord.asInstanceOf[Array[Double]](mIndex) - low.coord.asInstanceOf[Array[Double]](mIndex))
          } else if (high.coord.asInstanceOf[Array[Double]](mIndex) < other.low.coord.asInstanceOf[Array[Double]](mIndex)) {
            x = Math.abs(other.low.coord.asInstanceOf[Array[Double]](mIndex) - high.coord.asInstanceOf[Array[Double]](mIndex))
          }
          ans += x * mValue
      }
    } else {
      for (i <- low.coord.asInstanceOf[Array[Double]].indices) {
        var x = 0.0
        if (other.high.coord.asInstanceOf[Array[Double]](i) < low.coord.asInstanceOf[Array[Double]](i)) {
          x = Math.abs(other.high.coord.asInstanceOf[Array[Double]](i) - low.coord.asInstanceOf[Array[Double]](i))
        } else if (high.coord.asInstanceOf[Array[Double]](i) < other.low.coord.asInstanceOf[Array[Double]](i)) {
          x = Math.abs(other.low.coord.asInstanceOf[Array[Double]](i) - high.coord.asInstanceOf[Array[Double]](i))
        }
        ans += x
      }
    }
    ans
  }

  def approxMinDist(other: Shape): Double = {
    other match {
      case p: Point[Any] => approxMinDist(p)
    }
  }

  def approxMinDist(p: Point[Any]): Double = {
    var ans = 0.0
    for (i <- p.coord.asInstanceOf[Array[Double]].indices) {
      if (p.coord.asInstanceOf[Array[Double]](i) < low.coord.asInstanceOf[Array[Double]](i)) {
        ans = math.max(ans, low.coord.asInstanceOf[Array[Double]](i) - p.coord.asInstanceOf[Array[Double]](i))
      } else if (p.coord.asInstanceOf[Array[Double]](i) > high.coord.asInstanceOf[Array[Double]](i)) {
        ans = math.max(ans, p.coord.asInstanceOf[Array[Double]](i) - high.coord.asInstanceOf[Array[Double]](i))
      }
    }

    ans
  }

  def approxMinDist(other: Rectangle): Double = {
    var ans = 0.0
    for (i <- low.coord.asInstanceOf[Array[Double]].indices) {
      var x = 0.0
      if (other.high.coord.asInstanceOf[Array[Double]](i) < low.coord.asInstanceOf[Array[Double]](i)) {
        x = Math.abs(other.high.coord.asInstanceOf[Array[Double]](i) - low.coord.asInstanceOf[Array[Double]](i))
      } else if (high.coord.asInstanceOf[Array[Double]](i) < other.low.coord.asInstanceOf[Array[Double]](i)) {
        x = Math.abs(other.low.coord.asInstanceOf[Array[Double]](i) - high.coord.asInstanceOf[Array[Double]](i))
      }
      ans = math.max(ans, x)
    }

    ans
  }

  def volume: Double = (high - low).coord.asInstanceOf[Array[Double]].product

  override def toString: String = "(" + low.toString + "," + high.toString + ")"
}
