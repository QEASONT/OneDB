
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

package org.apache.spark.sql.execution.odb.exec.search

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.odb.ODBSimilarityFunction
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.{MetricData, ODBSimilarity}
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.{Point, Shape}
// import org.apache.spark.sql.catalyst.expressions.mchord.common.
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.odb.algorithms.MetricSimilarityWithKNNAlgorithms
import org.apache.spark.sql.execution.odb.exec.MetricExecUtils
import org.apache.spark.sql.execution.odb.sql.ODBIternalRow
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class ODBSimilarityWithKNNSearchExec(leftQuery: MetricData, rightKey: Expression,
                                          count: Int, queryM: Array[Int],
                                          rightLogicalPlan: LogicalPlan,
                                          right: SparkPlan)
  extends UnaryExecNode with Logging {

  override def output: Seq[Attribute] = right.output

  override def child: SparkPlan = right
  //  TODO: register kryo classes
  sparkContext.conf.registerKryoClasses(Array(classOf[Shape],
    classOf[Point[Array[Double]]],
    classOf[Point[String]])
  )

  protected override def doExecute(): RDD[InternalRow] = {
    logWarning(s"Count: $count")

    val rightResults = right.execute()
    val rightCount = rightResults.count()
    logWarning(s"Data count: $rightCount")

    logWarning("Applying efficient metric similarity search algorithm!")

    val rightMchordRDD = MetricExecUtils.getODBRDD(sqlContext, rightResults,
      rightKey, rightLogicalPlan, right)

    // get answer
    val search = MetricSimilarityWithKNNAlgorithms.DistributedSearch
    val answerRDD = search.search(sparkContext, leftQuery,
      rightMchordRDD, count, queryM)

    val outputRDD = answerRDD.map(x => x._1.asInstanceOf[ODBIternalRow].row)

    outputRDD.asInstanceOf[RDD[InternalRow]]
  }
}
