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

package org.apache.spark.sql.execution.odb.exec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolver
import org.apache.spark.sql.catalyst.expressions.odb.{ODBSimilarityExpression, ODBSimilarityFunction}
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.index.IndexedRelation
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.odb.rdd.ODBRDD
import org.apache.spark.sql.execution.odb.sql.{ODBIndexedRelation, ODBIternalRow}

object MetricExecUtils {
  // TODO: This is a temporary solution to get the indexed relation.
  def getIndexedRelation(sqlContext: SQLContext, plan: LogicalPlan): Option[IndexedRelation] = {
    //    val aaa = sqlContext.sessionState.odbIndexRegistry.lookupIndex(plan).map(_.relation)
    //    sqlContext.sessionState.odbIndexRegistry.lookupIndex(plan).map(_.relation)
    sqlContext.sessionState.odbIndexRegistry.getIndex().map(_.relation)
  }

  def getODBRDD(sqlContext: SQLContext, rdd: RDD[InternalRow], key: Expression,
                logicalPlan: LogicalPlan,
                physicalPlan: SparkPlan): ODBRDD = {
    getIndexedRelation(sqlContext, logicalPlan)
      .map(_.asInstanceOf[ODBIndexedRelation].odbRDD)
      .getOrElse({
        // QT: get or else means if there is no indexed relation, we need to build the index.
        //        val metricMArray = rdd.take(1).asInstanceOf[UnsafeRow].getArray(3).toIntArray

        //        val leftRDD = rdd.asInstanceOf[RDD[UnsafeRow]].map(row => {
        //          val pointsArray = ODBSimilarityExpression.getPoints(row)
        //          new ODBIternalRow(row.copy(), pointsArray)
        //        }).asInstanceOf[RDD[MetricData]]
        //        val metricMArray = leftRDD.take(1)(0).points.map(x => x.metricValue)
        //        new ODBRDD(leftRDD, metricMArray)
        val attribute = {
          val exprOptionMetricMaxDis = logicalPlan.output.find(attr => resolver(attr.name, "metricMaxDis"))
          exprOptionMetricMaxDis.getOrElse(throw new AnalysisException(s"Column metricMaxDis does not exist."))
        }

        val indexedRelation = ODBIndexedRelation(rdd, attribute)()
        val catalog = sqlContext.sparkSession.sessionState.catalog
        if (logicalPlan.children.nonEmpty) {
          val project = sqlContext.sparkSession.sessionState.optimizer.execute(
            Project(Seq(attribute), logicalPlan.children.head))
          catalog.createIndex("multimodel", "metric_index1", project, indexedRelation)
          indexedRelation.odbRDD
        } else {
//          val project = sqlContext.sparkSession.sessionState.optimizer.execute(
//            Project(Seq(attribute), logicalPlan.children.head))
          catalog.createIndex("multimodel", "metric_index1", logicalPlan, indexedRelation)
          indexedRelation.odbRDD
        }


      })
  }
}
