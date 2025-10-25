
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.odb.{ODBSimilarityExpression, ODBSimilarityFunction}
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.ODBSimilarity
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

case class CreateODBIndexCommand(tableName: TableIdentifier, columnDouble: String, columnString: String, columnMetricM: String, columnMaxDis: String, indexName: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val table = sparkSession.table(tableName)
    CreateODBIndexCommand.createODBIndex(sparkSession, table.logicalPlan, columnDouble, columnString, columnMetricM, columnMaxDis, indexName,
      Some(tableName))
    Seq.empty[Row]
  }
}

object CreateODBIndexCommand extends Logging {
  def createODBIndex(sparkSession: SparkSession,
                     logicalPlan: LogicalPlan,
                     columnDouble: String,
                     columnString: String,
                     columnMetricM: String,
                     columnMetricMaxDis: String,
                     indexName: String, tableName: Option[TableIdentifier]): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val attribute = {
      val resolver = sparkSession.sessionState.conf.resolver
      val exprOptionDouble = logicalPlan.output.find(attr => resolver(attr.name, columnDouble))
      exprOptionDouble.getOrElse(throw new AnalysisException(s"Column $columnDouble does not exist."))
      val exprOptionString = logicalPlan.output.find(attr => resolver(attr.name, columnString))
      exprOptionString.getOrElse(throw new AnalysisException(s"Column $columnString does not exist."))
      val exprOptionMetricM = logicalPlan.output.find(attr => resolver(attr.name, columnMetricM))
      exprOptionMetricM.getOrElse(throw new AnalysisException(s"Column $columnMetricM does not exist."))
      val exprOptionMetricMaxDis = logicalPlan.output.find(attr => resolver(attr.name, columnMetricMaxDis))
      exprOptionMetricMaxDis.getOrElse(throw new AnalysisException(s"Column $columnMetricMaxDis does not exist."))
    }
    val project = sparkSession.sessionState.optimizer.execute(
      Project(Seq(attribute), logicalPlan.children.head))

    if (catalog.lookupODBIndex(tableName, indexName, project).isEmpty) {
      val child = sparkSession.sessionState.executePlan(logicalPlan).sparkPlan
      val indexedRelation = ODBIndexedRelation(child, attribute)()
      catalog.createIndex(tableName, indexName, project, indexedRelation)
    } else {
      logWarning(s"Index $indexName on Table ${tableName.getOrElse("EMPTY")} exists!")
    }
  }
}