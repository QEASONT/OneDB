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

package org.apache.spark.sql.execution.odb.index.hnsw

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point

/**
 * Base class for brute-force KNN implementation, only used for benchmarking our HNSW
 * **note**: this implementation is not thread-safe
 *
 * @param dimension   number of dimension of all vectors in this model
 * @param df          the distance function
 * @param distanceOrd the distance ordering, used to compare distance
 */
class BruteForce(
                  override val distanceOrd: Ordering[Double]
                ) extends BaseGraph {

  private val candidateOrd = Candidate.fromDistanceOrd(distanceOrd)
  private val vectors = new ArrayBuffer[Point[Any]]()
  private var nextIndex = 0

  override def add(vector: Point[Any]): Unit = {
    vectors += vector
    nextIndex += 1
  }

  override def adjacencyListToCSR(): (Array[Int], Array[Int]) = {
    val xadj = new ArrayBuffer[Int]()
    val adjncy = new ArrayBuffer[Int]()
    xadj += 0
    for (i <- vectors.indices) {
      xadj += (xadj.last + 1)
      for (j <- vectors.indices) {
        if (i != j) {
          adjncy += j
        }
      }
    }
    (xadj.toArray, adjncy.toArray)
  }

  override def knn(vector: Point[Any], k: Int): Seq[Point[Any]] = {

    val distances = new ArrayBuffer[Candidate]()
    for (i <- vectors.indices) {
      distances += Candidate(i, vectors(i).minDist(vector))
    }
    distances.sorted(candidateOrd).take(k).map(c => vectors(c.index)).toList
  }

  override def knnWithDis(vector: Point[Any], k: Int): Seq[(Point[Any], Double)] = {

    val distances = new ArrayBuffer[Candidate]()
    for (i <- vectors.indices) {
      distances += Candidate(i, vectors(i).minDist(vector))
    }
    distances.sorted(candidateOrd).take(k).map(c => (vectors(c.index), c.distance)).toList
  }
}
