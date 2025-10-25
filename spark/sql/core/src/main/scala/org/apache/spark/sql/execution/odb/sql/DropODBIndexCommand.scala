
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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

case class DropODBIndexCommand(tableName: TableIdentifier, indexName: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    DropODBIndexCommand.dropODBIndex(sparkSession, indexName, Some(tableName))
    Seq.empty[Row]
  }
}

object DropODBIndexCommand {

  def dropODBIndex(sparkSession: SparkSession, indexName: String,
                    tableName: Option[TableIdentifier]): Unit = {
    val catalog = sparkSession.sessionState.catalog
    catalog.dropODBIndex(tableName, indexName)
  }

  def dropODBIndex(sparkSession: SparkSession, logicalPlan: LogicalPlan,
                    column: String): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val attribute = {
      val resolver = sparkSession.sessionState.conf.resolver
      val exprOption = logicalPlan.output.find(attr => resolver(attr.name, column))
      exprOption.getOrElse(throw new AnalysisException(s"Column $column does not exist."))
    }
    val project = sparkSession.sessionState.optimizer.execute(
      Project(Seq(attribute), logicalPlan.children.head))
    catalog.dropODBIndex(project)
  }
}