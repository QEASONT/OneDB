
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
package org.apache.spark.sql.execution.odb.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.{MetricData, ODBSimilarity}
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.odb.index.{GlobalIndex, IndexedRelation}
import org.apache.spark.sql.catalyst.expressions.odb.{ODBSimilarityExpression, ODBSimilarityFunction, PackedPartition}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.odb.rdd.ODBRDD

case class ODBIndexedRelation(child: Any, key: Attribute)
                             (var odbRDD: ODBRDD = null)
  extends IndexedRelation with MultiInstanceRelation {

  if (odbRDD == null) {
    odbRDD = buildIndex()
  }

  override def indexedRDD: RDD[PackedPartition] = odbRDD.packedRDD

  override def globalIndex: GlobalIndex = odbRDD.globalODBIndex

  override def newInstance(): IndexedRelation = {
    ODBIndexedRelation(child, key)(odbRDD).asInstanceOf[this.type]
  }

  private def buildIndex(): ODBRDD = child match {
    // QT: InternalRow or UnsafeRow
    case rdd: RDD[InternalRow] =>
      val leftRDD = rdd.asInstanceOf[RDD[UnsafeRow]].map(row => {
        val pointsArray = ODBSimilarityExpression.getPoints(row)
        new ODBIternalRow(row.copy(), pointsArray)
      }).asInstanceOf[RDD[MetricData]]
      //      val metricMArray = leftRDD.take(1)(0).points.map(x => x.metricValue)
      new ODBRDD(leftRDD)

    case sparkPlan: SparkPlan =>
      //      val metricMArray = sparkPlan.execute().asInstanceOf[RDD[UnsafeRow]].first().getArray(3).toIntArray
      val dataRDD = sparkPlan.execute().asInstanceOf[RDD[UnsafeRow]].map(row => {
        val pointsArray = ODBSimilarityExpression.getPoints(row)
        new ODBIternalRow(row.copy(), pointsArray)
      }).asInstanceOf[RDD[MetricData]]
      new ODBRDD(dataRDD)

  }
}

