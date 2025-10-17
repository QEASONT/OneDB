
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
package org.apache.spark.sql.execution.mbt.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.mbt.common.MBTConfigConstants
import org.apache.spark.sql.catalyst.expressions.mbt.common.metric.MBTSimilarity
import org.apache.spark.sql.catalyst.expressions.mbt.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.mbt.index.{GlobalIndex, IndexedRelation}
import org.apache.spark.sql.catalyst.expressions.mbt.{MBTSimilarityExpression, MBTSimilarityFunction, PackedPartition}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.mbt.rdd.MBTRDD

case class MBTIndexedRelation(distanceFunction: MBTSimilarityFunction, child: SparkPlan, key: Attribute)
                             (var mbtRDD: MBTRDD = null)
  extends IndexedRelation with MultiInstanceRelation {

  if (mbtRDD == null) {
    mbtRDD = buildIndex()
  }

  override def indexedRDD: RDD[PackedPartition] = mbtRDD.packedRDD

  override def globalIndex: GlobalIndex = mbtRDD.globalIndex

  override def newInstance(): IndexedRelation = {
    MBTIndexedRelation(distanceFunction, child, key)(mbtRDD).asInstanceOf[this.type]
  }

  private def buildIndex(): MBTRDD = {
    val dataRDD = child.execute().asInstanceOf[RDD[UnsafeRow]].map(row =>
      new MBTIternalRow(row,
        MBTSimilarityExpression.getPoints(row, child.schema))).asInstanceOf[RDD[Point[Any]]]
    new MBTRDD(dataRDD, distanceFunction)
  }
}

