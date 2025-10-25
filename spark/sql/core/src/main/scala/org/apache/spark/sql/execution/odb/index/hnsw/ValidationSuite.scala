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

import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point

class ValidationSuite[T, D](
                             val distanceOrd: Ordering[Double]
                           ) {

  /**
   * calculate model recall
   *
   * @param answers set of answers vectors
   * @param results the actual answers (or results)
   * @return recall
   */
  private def calculateRecall(answers: Seq[Point[Any]], results: Seq[Point[Any]]): Double = {
    val resultSet = results.toSet
    val truePositive = answers.count(ans => resultSet.contains(ans))
    val recall = truePositive.toDouble / answers.size
    recall
  }

  /**
   * perform model validation
   *
   * @param dataset           the input dataset, used to train the model
   * @param validationDataset the validation dataset
   * @param k                 number of neighbors to query
   * @param model             the input model to validate
   * @param numThreads        number of thread to run the validation
   * @return recall
   */
  def validate(
                dataset: Seq[Point[Any]],
                validationDataset: Seq[Point[Any]],
                k: Int,
                model: BaseGraph,
                numThreads: Int
              ): Double = {
    // assertions

    // building models
    val validationModel = new BruteForce(distanceOrd)
    dataset.foreach { v =>
      validationModel.add(v)
      model.add(v)
    }
    // split validation dataset
    val validationDatasetSize = validationDataset.size
    val subValidationSetSize = (validationDatasetSize.toDouble / numThreads).round.toInt
    val split = validationDataset.grouped(subValidationSetSize).toArray
    //    Utils.assert(split.length == numThreads, s"unexpected split size, expected = $numThreads, actual = ${split.length}")
    val pool = Executors.newFixedThreadPool(numThreads)
    val ec = ExecutionContext.fromExecutor(pool)
    val recallFut = new ArrayBuffer[Future[Double]]()
    for (i <- 0 until numThreads) {
      recallFut += Future {
        var recall = 0.0
        val queries = split(i)
        queries.foreach { query =>
          val results = validationModel.knn(query, k)
          val answers = model.knn(query, k)
          recall += calculateRecall(answers, results) / validationDatasetSize
        }
        recall
      }(ec)
    }
    implicit val joinEc: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
    val f = Future.sequence(recallFut).map(_.sum)
    val recall = Await.result(f, Duration.Inf)
    pool.shutdown()
    recall
  }
}
