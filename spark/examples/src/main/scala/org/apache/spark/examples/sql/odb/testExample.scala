package org.apache.spark.examples.sql.odb

import org.apache.spark.sql.execution.odb.index.MVPTree.MVPTree
import org.apache.spark.sql.execution.odb.index.entity.{MVPDP, MVPError}
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point

import scala.collection.JavaConverters.seqAsJavaListConverter

object testExample {
  def main(args: Array[String]) {
    val mvpTreeForest = new Array[MVPTree](1)
    // random dataset
    val bf = 2
    val p = 20
    val k = 25

    val randomData = Array.fill(1000)(Array.fill(10)(Math.random()))

    mvpTreeForest(0) = new MVPTree(bf, p, k, 0, 0, 0, 0, null)
    val mvpdpArray = randomData.zipWithIndex.map { case (data, index) =>

      new MVPDP(index, Point[Any](data, 0, 0, 1))
    }

    mvpTreeForest(0).mvpAdd(new java.util.ArrayList(mvpdpArray.toList.asJava), randomData.length)
    val queryPoint = Point[Any](Array.fill(10)(Math.random()), 0, 0, 1)
    val mvpdp = new MVPDP(0, queryPoint)
    val res1 = mvpTreeForest(0).mvpKnnSearch(mvpdp, k)
    val error = MVPError.MVP_SUCCESS
    val res2 = mvpTreeForest(0).mvpRetrieve(mvpdp, 1, error)
    val aaa = 0

  }
}
