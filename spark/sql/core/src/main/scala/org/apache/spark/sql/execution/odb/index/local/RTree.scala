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


case class RTreeNode(m_mbr: Rectangle, m_child: Array[RTreeEntry], isLeaf: Boolean) {
  def this(m_mbr: Rectangle, children: Array[(Rectangle, RTreeNode)]) = {
    this(m_mbr, children.map(x => RTreeInternalEntry(x._1, x._2)), false)
  }

  def this(m_mbr: Rectangle, children: => Array[(Long, Point[Any])]) = {
    this(m_mbr, children.map(x => RTreeLeafEntry(x._1, x._2, 1)), true)
  }

  def this(m_mbr: Rectangle, children: Array[(Rectangle, Int, Int)]) = {
    this(m_mbr, children.map(x => RTreeLeafEntry(x._2, x._1, x._3)), true)
  }

  val size: Long = {
    if (isLeaf) {
      m_child.map(x => x.asInstanceOf[RTreeLeafEntry].size).sum
    } else {
      m_child.map(x => x.asInstanceOf[RTreeInternalEntry].node.size).sum
    }
  }
}

abstract class RTreeEntry extends Shape {
  def minDist(x: Any, queryM: Array[(Double, Int)]): Double

  def intersects(x: Shape): Boolean
}

case class RTreeLeafEntry(m_data: Long, shape: Shape, size: Int) extends RTreeEntry {
  override def minDist(x: Any, queryM: Array[(Double, Int)]): Double = shape.minDist(x)

  override def dist(x: Any, queryM: Array[(Double, Int)]): Double = -1

  override def intersects(x: Shape): Boolean = x.intersects(shape)
}

case class RTreeInternalEntry(mbr: Rectangle, node: RTreeNode) extends RTreeEntry {
  override def minDist(x: Any, queryM: Array[(Double, Int)]): Double = mbr.minDist(x)

  override def dist(x: Any, queryM: Array[(Double, Int)]): Double = -1

  override def intersects(x: Shape): Boolean = x.intersects(mbr)
}

case class RTree(root: RTreeNode) extends LocalIndex with Serializable {
  // Insert a new entry into the R-Tree
  def insert(newEntry: (Long, Point[Any]), maxEntriesPerNode: Int): RTree = {
    def chooseLeaf(node: RTreeNode, entry: (Long, Point[Any])): RTreeNode = {
      if (node.isLeaf) {
        node
      } else {
        // Find the child whose MBR requires the least enlargement
        val bestChild = node.m_child.map(_.asInstanceOf[RTreeInternalEntry])
          .minBy { child =>
            val currentArea = child.mbr.volume
            val enlargedArea = child.mbr.expandToInclude(entry._2).volume
            enlargedArea - currentArea
          }
        chooseLeaf(bestChild.node, entry)
      }
    }

    def adjustTree(node: RTreeNode): RTreeNode = {
      if (node.m_child.length <= maxEntriesPerNode) {
        node
      } else {
        // Split the node if it exceeds the maximum entries
        val (left, right) = splitNode(node)
        RTreeNode(
          m_mbr = left.m_mbr.expandToInclude(right.m_mbr),
          m_child = Array(
            RTreeInternalEntry(left.m_mbr, left),
            RTreeInternalEntry(right.m_mbr, right)
          ),
          isLeaf = false
        )
      }
    }

    def splitNode(node: RTreeNode): (RTreeNode, RTreeNode) = {
      val entries = node.m_child
      val (leftEntries, rightEntries) = entries.splitAt(entries.length / 2)

      val leftMBR = leftEntries.map {
        case leaf: RTreeLeafEntry => leaf.shape.asInstanceOf[Rectangle]
        case internal: RTreeInternalEntry => internal.mbr
      }.reduce(_ expandToInclude _)

      val rightMBR = rightEntries.map {
        case leaf: RTreeLeafEntry => leaf.shape.asInstanceOf[Rectangle]
        case internal: RTreeInternalEntry => internal.mbr
      }.reduce(_ expandToInclude _)

      (
        RTreeNode(leftMBR, leftEntries, node.isLeaf),
        RTreeNode(rightMBR, rightEntries, node.isLeaf)
      )
    }

    val leaf = chooseLeaf(root, newEntry)

    // Add the new entry to the chosen leaf
    val updatedLeaf = RTreeNode(
      leaf.m_mbr.expandToInclude(newEntry._2),
      leaf.m_child :+ RTreeLeafEntry(newEntry._1, newEntry._2, 1),
      isLeaf = true
    )

    // Adjust the tree from the leaf up to the root
    val updatedRoot = adjustTree(updatedLeaf)

    RTree(updatedRoot)
  }

  def delete(entryToDelete: (Long, Point[Any])): RTree = {
    // Helper function to find the leaf node containing the entry
    def findLeaf(node: RTreeNode, entry: (Long, Point[Any])): Option[RTreeNode] = {
      if (node.isLeaf) {
        if (node.m_child.exists {
          case RTreeLeafEntry(m_data, shape, _) =>
            shape.equals(entry._2) && m_data == entry._1
          case _ => false
        }) {
          Some(node)
        } else {
          None
        }
      } else {
        node.m_child.collectFirst {
          case RTreeInternalEntry(_, childNode) if findLeaf(childNode, entry).isDefined =>
            findLeaf(childNode, entry).get
        }
      }
    }

    // Adjust the tree by removing the entry and reinserting orphaned nodes
    def adjustTree(node: RTreeNode, orphanedNodes: mutable.ArrayBuffer[RTreeNode]): RTreeNode = {
      if (node.m_child.isEmpty) {
        orphanedNodes += node
        null
      } else {
        val updatedMBR = node.m_child.map {
          case RTreeLeafEntry(shape, _, _) => shape.asInstanceOf[Rectangle]
          case RTreeInternalEntry(mbr, _) => mbr
        }.reduce(_ expandToInclude _)

        RTreeNode(updatedMBR, node.m_child, node.isLeaf)
      }
    }

    // Delete the entry and adjust the tree
    def deleteEntry(node: RTreeNode, entry: (Long, Point[Any])): (RTreeNode, mutable.ArrayBuffer[RTreeNode]) = {
      val orphanedNodes = mutable.ArrayBuffer[RTreeNode]()
      if (node.isLeaf) {
        val updatedChildren = node.m_child.filterNot {
          case RTreeLeafEntry(m_data, shape, _) =>
            shape.equals(entry._1) && m_data == entry._2
          case _ => false
        }

        val updatedNode = RTreeNode(node.m_mbr, updatedChildren, isLeaf = true)
        (adjustTree(updatedNode, orphanedNodes), orphanedNodes)
      } else {
        val updatedChildren = node.m_child.flatMap {
          case entry: RTreeInternalEntry =>
            val (updatedChildNode, orphaned) = deleteEntry(entry.node, entryToDelete)
            orphanedNodes ++= orphaned
            //            if (updatedChildNode != null) Some(RTreeInternalEntry(updatedChildNode.m_mbr, updatedChildNode)) else None
            if (updatedChildNode != null) Some(RTreeInternalEntry(updatedChildNode.m_mbr, updatedChildNode)) else None
        }

        val updatedNode = RTreeNode(node.m_mbr, updatedChildren.asInstanceOf[Array[RTreeEntry]], isLeaf = false)
        (adjustTree(updatedNode, orphanedNodes), orphanedNodes)
      }
    }

    // Main logic for delete
    val (updatedRoot, orphanedNodes) = deleteEntry(root, entryToDelete)

    // Reinsert orphaned nodes
    orphanedNodes.foreach { orphan =>
      orphan.m_child.foreach {
        case RTreeLeafEntry(m_data, shape, _) => this.insert((m_data, shape.asInstanceOf[Point[Any]]), 4)
        case _ => // Internal nodes are handled recursively
      }
    }

    RTree(updatedRoot)
  }


  def range(query: Rectangle): Array[(Long, Shape)] = {
    val ans = mutable.ArrayBuffer[(Long, Shape)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.intersects(query) && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach {
          case RTreeInternalEntry(mbr, node) =>
            if (query.intersects(mbr)) st.push(node)
        }
      } else {
        now.m_child.foreach {
          case RTreeLeafEntry(m_data, shape, _) =>
            if (query.intersects(shape)) ans += ((m_data, shape))
        }
      }
    }
    ans.toArray
  }

  def circleRange(origin: Shape, r: Double): List[(Shape, Double)] = {
    val ans = mutable.ListBuffer[(Shape, Double)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.dist(origin) <= r && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach {
          case RTreeInternalEntry(mbr, node) =>
            val dis_tmp = origin.dist(mbr) / origin.asInstanceOf[Point[Any]].metricMaxDis
            if (dis_tmp <= r) st.push(node)
        }
      } else {
        now.m_child.foreach {
          case RTreeLeafEntry(m_data, shape, _) =>
            val dis_tmp = origin.dist(shape)
            if (dis_tmp <= r) ans += ((shape, dis_tmp))
        }
      }
    }
    ans.toList
  }
}

object RTree {
  def apply(entries: Array[(Long, Point[Any])], max_entries_per_node: Int): RTree = {
    val dimension = entries(0)._2.coord.asInstanceOf[Array[Double]].length
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def recursiveGroupPoint[Any](entries: Array[(Long, Point[Any])],
                                 cur_dim: Int, until_dim: Int): Array[Array[(Long, Point[Any])]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(_._2.coord.asInstanceOf[Array[Double]](cur_dim) < _._2.coord.asInstanceOf[Array[Double]](cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupPoint[Any](now, cur_dim + 1, until_dim))
      } else {
        grouped
      }
    }

    val grouped = recursiveGroupPoint[Any](entries, 0, dimension - 1)
    val rtree_nodes = mutable.ArrayBuffer[(Rectangle, RTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(_ => Double.MaxValue)
      val max = new Array[Double](dimension).map(_ => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 until dimension) min(i) = Math.min(min(i), now._2.coord.asInstanceOf[Array[Double]](i))
        for (i <- 0 until dimension) max(i) = Math.max(max(i), now._2.coord.asInstanceOf[Array[Double]](i))
      })
      val demoPoint = list.take(1).head._2
      val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue), Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
      rtree_nodes += ((mbr, new RTreeNode(mbr, list)))
    })

    var cur_rtree_nodes = rtree_nodes.toArray
    var cur_len = cur_rtree_nodes.length.toDouble
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

    def comp(dim: Int)(left: (Rectangle, RTreeNode), right: (Rectangle, RTreeNode)): Boolean = {
      val left_center = left._1.low.coord.asInstanceOf[Array[Double]](dim) + left._1.high.coord.asInstanceOf[Array[Double]](dim)
      val right_center = right._1.low.coord.asInstanceOf[Array[Double]](dim) + right._1.high.coord.asInstanceOf[Array[Double]](dim)
      left_center < right_center
    }

    def recursiveGroupRTreeNode(entries: Array[(Rectangle, RTreeNode)], cur_dim: Int,
                                until_dim: Int)
    : Array[Array[(Rectangle, RTreeNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupRTreeNode(now, cur_dim + 1, until_dim))
      } else {
        grouped
      }
    }

    while (!over(dim)) {
      val grouped = recursiveGroupRTreeNode(cur_rtree_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(Rectangle, RTreeNode)]()
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
        tmp_nodes += ((mbr, new RTreeNode(mbr, list)))
      })
      cur_rtree_nodes = tmp_nodes.toArray
      cur_len = cur_rtree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 until dimension) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }
    val demoPoint = cur_rtree_nodes.take(1).head._1.low
    val min = new Array[Double](dimension).map(_ => Double.MaxValue)
    val max = new Array[Double](dimension).map(_ => Double.MinValue)
    cur_rtree_nodes.foreach(now => {
      for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord.asInstanceOf[Array[Double]](i))
      for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord.asInstanceOf[Array[Double]](i))
    })

    val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue),
      Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
    val root = new RTreeNode(mbr, cur_rtree_nodes)
    new RTree(root)
  }

  def apply(entries: Array[(Rectangle, Int, Int)], max_entries_per_node: Int): RTree = {
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

    def recursiveGroupMBR(entries: Array[(Rectangle, Int, Int)], cur_dim: Int, until_dim: Int)
    : Array[Array[(Rectangle, Int, Int)]] = {
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
    val rtree_nodes = mutable.ArrayBuffer[(Rectangle, RTreeNode)]()
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
      rtree_nodes += ((mbr, new RTreeNode(mbr, list)))
    })

    var cur_rtree_nodes = rtree_nodes.toArray
    var cur_len = cur_rtree_nodes.length.toDouble
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

    def comp(dim: Int)(left: (Rectangle, RTreeNode), right: (Rectangle, RTreeNode)): Boolean = {
      val left_center = left._1.low.coord.asInstanceOf[Array[Double]](dim) + left._1.high.coord.asInstanceOf[Array[Double]](dim)
      val right_center = right._1.low.coord.asInstanceOf[Array[Double]](dim) + right._1.high.coord.asInstanceOf[Array[Double]](dim)
      left_center < right_center
    }

    def recursiveGroupRTreeNode(entries: Array[(Rectangle, RTreeNode)],
                                cur_dim: Int, until_dim: Int): Array[Array[(Rectangle, RTreeNode)
    ]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => {
          recursiveGroupRTreeNode(now, cur_dim + 1, until_dim)
        })
      } else {
        grouped
      }
    }

    while (!over(dim)) {
      val grouped = recursiveGroupRTreeNode(cur_rtree_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(Rectangle, RTreeNode)]()
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
        tmp_nodes += ((mbr, new RTreeNode(mbr, list)))
      })
      cur_rtree_nodes = tmp_nodes.toArray
      cur_len = cur_rtree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 until dimension) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }

    val min = new Array[Double](dimension).map(_ => Double.MaxValue)
    val max = new Array[Double](dimension).map(_ => Double.MinValue)
    cur_rtree_nodes.foreach(now => {
      for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord.asInstanceOf[Array[Double]](i))
      for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord.asInstanceOf[Array[Double]](i))
    })
    val demoPoint = cur_rtree_nodes.take(1).head._1.low
    val mbr = Rectangle(Point[Any](min, demoPoint.metricIndex, demoPoint.metricValue),
      Point[Any](max, demoPoint.metricIndex, demoPoint.metricValue))
    val root = new RTreeNode(mbr, cur_rtree_nodes)
    new RTree(root)
  }
}