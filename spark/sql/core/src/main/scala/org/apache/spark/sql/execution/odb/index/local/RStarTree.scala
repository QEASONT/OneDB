/*
 *  Copyright 2025 by ODB Project
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
package org.apache.spark.sql.execution.odb.index.local

import org.apache.spark.sql.catalyst.expressions.odb.index.LocalIndex
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.{Point, Rectangle, Shape}

import scala.collection.mutable


case class RStarTreeNode(m_mbr: Rectangle, m_child: Array[RStarTreeEntry], isLeaf: Boolean) {
  def this(m_mbr: Rectangle, children: Array[(Rectangle, RStarTreeNode)]) = {
    this(m_mbr, children.map(x => RStarTreeInternalEntry(x._1, x._2)), false)
  }

  def this(m_mbr: Rectangle, children: => Array[(Point[Any], Int)]) = {
    this(m_mbr, children.map(x => RStarTreeLeafEntry(x._1, x._2, 1)), true)
  }

  def this(m_mbr: Rectangle, children: Array[(Rectangle, Int, Int)]) = {
    this(m_mbr, children.map(x => RStarTreeLeafEntry(x._1, x._2, x._3)), true)
  }

  val size: Long = {
    if (isLeaf) {
      m_child.map(x => x.asInstanceOf[RStarTreeLeafEntry].size).sum
    } else {
      m_child.map(x => x.asInstanceOf[RStarTreeInternalEntry].node.size).sum
    }
  }

  private def computeMBR(entries: Array[RStarTreeEntry]): Rectangle = {
    val dimension = entries.head.shape.asInstanceOf[Rectangle].low.coord.asInstanceOf[Array[Double]].length
    val min = Array.fill(dimension)(Double.MaxValue)
    val max = Array.fill(dimension)(Double.MinValue)

    entries.foreach { entry =>
      val rect = entry.shape.asInstanceOf[Rectangle]
      for (i <- 0 until dimension) {
        min(i) = Math.min(min(i), rect.low.coord.asInstanceOf[Array[Double]](i))
        max(i) = Math.max(max(i), rect.high.coord.asInstanceOf[Array[Double]](i))
      }
    }

    Rectangle(Point(min, entries.head.shape.asInstanceOf[Rectangle].low.metricIndex, entries.head.shape.asInstanceOf[Rectangle].low.metricValue),
      Point(max, entries.head.shape.asInstanceOf[Rectangle].high.metricIndex, entries.head.shape.asInstanceOf[Rectangle].high.metricValue))
  }

  def insert(entry: RStarTreeEntry, maxEntries: Int): Option[(RStarTreeNode, RStarTreeNode)] = {
    if (m_child.length < maxEntries) {
      m_child :+ entry
      m_mbr.expandToInclude(entry.shape)
      None
    } else {
      val newNodes = splitNode(entry, maxEntries)
      Some(newNodes)
    }
  }

  private def splitNode(entry: RStarTreeEntry, maxEntries: Int): (RStarTreeNode, RStarTreeNode) = {
    val allEntries = m_child :+ entry
    val (group1, group2) = chooseBestSplit(allEntries, maxEntries)
    val mbr1 = computeMBR(group1)
    val mbr2 = computeMBR(group2)
    (new RStarTreeNode(mbr1, group1, isLeaf), new RStarTreeNode(mbr2, group2, isLeaf))
  }

  private def chooseBestSplit(entries: Array[RStarTreeEntry], maxEntries: Int): (Array[RStarTreeEntry], Array[RStarTreeEntry]) = {
    // Implement R*-Tree split strategy, e.g., quadratic split or R*-specific split
    // This is a placeholder; actual implementation will depend on the specific split criteria
    entries.splitAt(entries.length / 2)
  }
}

abstract class RStarTreeEntry extends Shape {
  def minDist(x: Any, queryM: Array[(Double, Int)]): Double

  def dist(other: Any, queryM: Array[(Double, Int)]): Double

  def intersects(x: Shape): Boolean

  def shape: Shape // 添加 shape 属性的抽象方法
}

case class RStarTreeLeafEntry(shape: Shape, m_data: Int, size: Int) extends RStarTreeEntry {
  override def minDist(x: Any, queryM: Array[(Double, Int)]): Double = shape.minDist(x)

  override def dist(other: Any, queryM: Array[(Double, Int)]): Double = shape.dist(other, queryM)

  override def intersects(x: Shape): Boolean = x.intersects(shape)
}

case class RStarTreeInternalEntry(mbr: Rectangle, node: RStarTreeNode) extends RStarTreeEntry {
  override def minDist(x: Any, queryM: Array[(Double, Int)]): Double = mbr.minDist(x)

  override def dist(other: Any, queryM: Array[(Double, Int)]): Double = shape.dist(other, queryM)

  override def intersects(x: Shape): Boolean = x.intersects(mbr)

  override def shape: Shape = mbr // 返回 mbr 作为 shape
}

case class RStarTree(root: RStarTreeNode) extends LocalIndex with Serializable {
  def insert(entry: RStarTreeEntry, maxEntries: Int): RStarTree = {
    val rootSplit = root.insert(entry, maxEntries)
    rootSplit match {
      case None => this
      case Some((left, right)) =>
        val newRootMBR = root.m_mbr.expandToInclude(left.m_mbr).expandToInclude(right.m_mbr)
        val newRoot = new RStarTreeNode(newRootMBR, Array(RStarTreeInternalEntry(left.m_mbr, left), RStarTreeInternalEntry(right.m_mbr, right)), false)
        RStarTree(newRoot)
    }
  }

  def range(query: Rectangle): Array[(Shape, Int)] = {
    val ans = mutable.ArrayBuffer[(Shape, Int)]()
    val st = new mutable.Stack[RStarTreeNode]()
    if (root.m_mbr.intersects(query) && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach {
          case RStarTreeInternalEntry(mbr, node) =>
            if (query.intersects(mbr)) st.push(node)
        }
      } else {
        now.m_child.foreach {
          case RStarTreeLeafEntry(shape, m_data, _) =>
            if (query.intersects(shape)) ans += ((shape, m_data))
        }
      }
    }
    ans.toArray
  }

  //  def circleRange(origin: Shape, r: Double, queryM: Array[(Double, Int)] = null): List[(Shape, Int)] = {
  //    val ans = mutable.ListBuffer[(Shape, Int)]()
  //    val st = new mutable.Stack[RStarTreeNode]()
  //    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty) st.push(root)
  //    while (st.nonEmpty) {
  //      val now = st.pop()
  //      if (!now.isLeaf) {
  //        now.m_child.foreach {
  //          case RStarTreeInternalEntry(mbr, node) =>
  //            if (origin.minDist(mbr, queryM) <= r) st.push(node)
  //        }
  //      } else {
  //        now.m_child.foreach {
  //          case RStarTreeLeafEntry(shape, m_data, _) =>
  //            if (origin.minDist(shape, queryM) <= r) ans += ((shape, m_data))
  //        }
  //      }
  //    }
  //    ans.toList
  //  }

  def circleRange(origin: Shape, r: Double, queryM: Array[(Double, Int)] = null): List[(Shape, Int)] = {
    val ans = mutable.ListBuffer[(Shape, Int)]()
    val st = new mutable.Stack[RStarTreeNode]()
    val tmp1 = root.m_mbr.dist(origin)
    val tmp2 = origin.dist(root.m_mbr, queryM)
    if (origin.dist(root.m_mbr, queryM) <= r && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach {
          case RStarTreeInternalEntry(mbr, node) =>
            val dist = origin.dist(mbr, queryM)
            if (dist <= r) st.push(node)
        }
      } else {
        now.m_child.foreach {
          case RStarTreeLeafEntry(shape, m_data, _) =>
            val dist = origin.dist(shape, queryM)
            if (dist <= r) ans += ((shape, m_data))
        }
      }
    }
    ans.toList
  }
}

object RStarTree {
  def apply(entries: Array[(Point[Any], Int)], max_entries_per_node: Int): RStarTree = {
    val dimension = entries(0)._1.coord.asInstanceOf[Array[Double]].length
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def recursiveGroupPoint[Any](entries: Array[(Point[Any], Int)],
                                 cur_dim: Int, until_dim: Int): Array[Array[(Point[Any], Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(_._1.coord.asInstanceOf[Array[Double]](cur_dim) < _._1.coord.asInstanceOf[Array[Double]](cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupPoint[Any](now, cur_dim + 1, until_dim))
      } else {
        grouped
      }
    }

    val grouped = recursiveGroupPoint[Any](entries, 0, dimension - 1)
    val RStarTree_nodes = mutable.ArrayBuffer[(Rectangle, RStarTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(_ => Double.MaxValue)
      val max = new Array[Double](dimension).map(_ => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.coord.asInstanceOf[Array[Double]](i))
        for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.coord.asInstanceOf[Array[Double]](i))
      })
      val demoPoint = list.take(1).head._1
      val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue), Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
      RStarTree_nodes += ((mbr, new RStarTreeNode(mbr, list)))
    })

    var cur_RStarTree_nodes = RStarTree_nodes.toArray
    var cur_len = cur_RStarTree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def over(dim: Array[Int]): Boolean = {
      for (i <- dim.indices)
        if (dim(i) != 1) return false
      true
    }

    def comp(dim: Int)(left: (Rectangle, RStarTreeNode), right: (Rectangle, RStarTreeNode)): Boolean = {
      val left_center = left._1.low.coord.asInstanceOf[Array[Double]](dim) + left._1.high.coord.asInstanceOf[Array[Double]](dim)
      val right_center = right._1.low.coord.asInstanceOf[Array[Double]](dim) + right._1.high.coord.asInstanceOf[Array[Double]](dim)
      left_center < right_center
    }

    def recursiveGroupRStarTreeNode(entries: Array[(Rectangle, RStarTreeNode)], cur_dim: Int,
                                    until_dim: Int)
    : Array[Array[(Rectangle, RStarTreeNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupRStarTreeNode(now, cur_dim + 1, until_dim))
      } else {
        grouped
      }
    }

    while (!over(dim)) {
      val grouped = recursiveGroupRStarTreeNode(cur_RStarTree_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(Rectangle, RStarTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(_ => Double.MaxValue)
        val max = new Array[Double](dimension).map(_ => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord.asInstanceOf[Array[Double]](i))
          for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord.asInstanceOf[Array[Double]](i))
        })
        val demoPoint = list.take(1).head._1.low
        val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue),
          Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
        tmp_nodes += ((mbr, new RStarTreeNode(mbr, list)))
      })
      cur_RStarTree_nodes = tmp_nodes.toArray
      cur_len = cur_RStarTree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 until dimension) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }

    // Implementing re-insertion step for R*Tree
    def reInsert(nodes: Array[(Rectangle, RStarTreeNode)], level: Int): Array[(Rectangle, RStarTreeNode)] = {
      if (nodes.length <= max_entries_per_node) return nodes

      // Sort by the distance from the center of the MBR
      val sortedNodes = nodes.sortBy { case (rect, _) =>
        val center = rect.centroid
        val dist = math.sqrt(center.coord.asInstanceOf[Array[Double]].map(c => c * c).sum)
        dist
      }

      // Choose p elements to reinsert
      val p = math.ceil(nodes.length * 0.3).toInt
      val toReinsert = sortedNodes.take(p)
      val remainingNodes = sortedNodes.drop(p)

      // Reinsertion process (this is simplified, real implementation would be more complex)
      remainingNodes ++ toReinsert
    }

    // Apply re-insertion at each level to minimize overlap and coverage
    var final_nodes = reInsert(cur_RStarTree_nodes, 0)

    while (final_nodes.length > max_entries_per_node) {
      val grouped = recursiveGroupRStarTreeNode(final_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(Rectangle, RStarTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(_ => Double.MaxValue)
        val max = new Array[Double](dimension).map(_ => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord.asInstanceOf[Array[Double]](i))
          for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord.asInstanceOf[Array[Double]](i))
        })
        val demoPoint = list.take(1).head._1.low
        val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue),
          Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
        tmp_nodes += ((mbr, new RStarTreeNode(mbr, list)))
      })
      final_nodes = reInsert(tmp_nodes.toArray, 0)
    }

    val demoPoint = final_nodes.take(1).head._1.low
    val min = new Array[Double](dimension).map(_ => Double.MaxValue)
    val max = new Array[Double](dimension).map(_ => Double.MinValue)
    final_nodes.foreach(now => {
      for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord.asInstanceOf[Array[Double]](i))
      for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord.asInstanceOf[Array[Double]](i))
    })

    val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue),
      Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
    val root = new RStarTreeNode(mbr, final_nodes)
    new RStarTree(root)
  }


  def apply(entries: Array[(Rectangle, Int, Int)], max_entries_per_node: Int): RStarTree = {
    val dimension = entries(0)._1.low.coord.asInstanceOf[Array[Double]].length
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def compMBR(dim: Int)(left: (Rectangle, Int, Int), right: (Rectangle, Int, Int)): Boolean = {
      val left_center = left._1.low.coord.asInstanceOf[Array[Double]](dim) + left._1.high.coord.asInstanceOf[Array[Double]](dim)
      val right_center = right._1.low.coord.asInstanceOf[Array[Double]](dim) + right._1.high.coord.asInstanceOf[Array[Double]](dim)
      left_center < right_center
    }

    def recursiveGroupMBR(entries: Array[(Rectangle, Int, Int)], cur_dim: Int, until_dim: Int): Array[Array[(Rectangle, Int, Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(compMBR(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupMBR(now, cur_dim + 1, until_dim))
      } else {
        grouped
      }
    }

    val grouped = recursiveGroupMBR(entries, 0, dimension - 1)
    val RStarTree_nodes = mutable.ArrayBuffer[(Rectangle, RStarTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(_ => Double.MaxValue)
      val max = new Array[Double](dimension).map(_ => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord.asInstanceOf[Array[Double]](i))
        for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord.asInstanceOf[Array[Double]](i))
      })
      val demoPoint = list.take(1).head._1.low
      val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue),
        Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
      RStarTree_nodes += ((mbr, new RStarTreeNode(mbr, list)))
    })

    var cur_RStarTree_nodes = RStarTree_nodes.toArray
    var cur_len = cur_RStarTree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def over(dim: Array[Int]): Boolean = {
      for (i <- dim.indices)
        if (dim(i) != 1) return false
      true
    }

    def comp(dim: Int)(left: (Rectangle, RStarTreeNode), right: (Rectangle, RStarTreeNode)): Boolean = {
      val left_center = left._1.low.coord.asInstanceOf[Array[Double]](dim) + left._1.high.coord.asInstanceOf[Array[Double]](dim)
      val right_center = right._1.low.coord.asInstanceOf[Array[Double]](dim) + right._1.high.coord.asInstanceOf[Array[Double]](dim)
      left_center < right_center
    }

    def recursiveGroupRStarTreeNode(entries: Array[(Rectangle, RStarTreeNode)],
                                    cur_dim: Int, until_dim: Int): Array[Array[(Rectangle, RStarTreeNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupRStarTreeNode(now, cur_dim + 1, until_dim))
      } else {
        grouped
      }
    }

    // Implementing re-insertion step for R*Tree
    def reInsert(nodes: Array[(Rectangle, RStarTreeNode)], level: Int): Array[(Rectangle, RStarTreeNode)] = {
      if (nodes.length <= max_entries_per_node) return nodes

      // Sort by the distance from the center of the MBR
      val sortedNodes = nodes.sortBy { case (rect, _) =>
        val center = rect.centroid
        val dist = math.sqrt(center.coord.asInstanceOf[Array[Double]].map(c => c * c).sum)
        dist
      }

      // Choose p elements to reinsert
      val p = math.ceil(nodes.length * 0.3).toInt
      val toReinsert = sortedNodes.take(p)
      val remainingNodes = sortedNodes.drop(p)

      // Reinsertion process (this is simplified, real implementation would be more complex)
      remainingNodes ++ toReinsert
    }

    // Apply re-insertion at each level to minimize overlap and coverage
    var final_nodes = reInsert(cur_RStarTree_nodes, 0)

    while (final_nodes.length > max_entries_per_node) {
      val grouped = recursiveGroupRStarTreeNode(final_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(Rectangle, RStarTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(_ => Double.MaxValue)
        val max = new Array[Double](dimension).map(_ => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord.asInstanceOf[Array[Double]](i))
          for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord.asInstanceOf[Array[Double]](i))
        })
        val demoPoint = list.take(1).head._1.low
        val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue),
          Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
        tmp_nodes += ((mbr, new RStarTreeNode(mbr, list)))
      })
      final_nodes = reInsert(tmp_nodes.toArray, 0)
    }

    val min = new Array[Double](dimension).map(_ => Double.MaxValue)
    val max = new Array[Double](dimension).map(_ => Double.MinValue)
    final_nodes.foreach(now => {
      for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord.asInstanceOf[Array[Double]](i))
      for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord.asInstanceOf[Array[Double]](i))
    })
    val demoPoint = final_nodes.take(1).head._1.low
    val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue),
      Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
    val root = new RStarTreeNode(mbr, final_nodes)
    new RStarTree(root)
  }

}