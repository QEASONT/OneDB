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

package org.apache.spark.sql.catalyst.expressions.mbt

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, UnaryExpression, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.mbt.common.MBTConfigConstants
import org.apache.spark.sql.catalyst.expressions.mbt.common.metric.MBTSimilarity
import org.apache.spark.sql.catalyst.expressions.mbt.common.shape.Point
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, FloatType, StringType, StructType}

import java.util.Locale

case class MBTSimilarityExpression(function: MBTSimilarityFunction,
                                   met1: Expression, met2: Expression)
  extends BinaryExpression with CodegenFallback {

  override def left: Expression = met1

  override def right: Expression = met2

  override def dataType: DataType = DoubleType

  override def nullSafeEval(m1: Any, m2: Any): Any = {
    val metric1 = m1 match {
      case t: Point[Any] => t
      case uad: UnsafeArrayData => MBTSimilarityExpression.getPoints(uad)
    }
    val metric2 = m2 match {
      case t: Point[Any] => t
      case uad: UnsafeArrayData => MBTSimilarityExpression.getPoints(uad)
    }
    MBTSimilarity.EUCLIDistance.evalWithPoint(metric1, metric2)
  }
}

object MBTSimilarityExpression {
  def getPoints(rawData: Any, schema: StructType = null): Point[Any] = {
    schema.last.dataType match {
      case ArrayType(DoubleType, false) =>
        Point(rawData.asInstanceOf[UnsafeRow].getArray(1).toDoubleArray)
      case StringType =>
        Point(rawData.asInstanceOf[UnsafeRow].getString(1))
      case _ =>
        throw new IllegalArgumentException(s"Unsupported metric similarity function '${schema.last.dataType}'. ")
    }
  }
}

case class MBTSimilarityWithKNNExpression(similarity: MBTSimilarityExpression,
                                          count: Int)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(left: Any): Any = {
    throw new NotImplementedError()
  }
}

sealed abstract class MBTSimilarityFunction extends Serializable {
  def sql: String
}

object MBTSimilarityFunction{
  case object EUCLID extends MBTSimilarityFunction {
    override def sql: String = "EUCLID"
  }

  case object L1 extends MBTSimilarityFunction {
    override def sql: String = "L1"
  }

  case object COSINE extends MBTSimilarityFunction {
    override def sql: String = "COSINE"
  }

  case object EDIT extends MBTSimilarityFunction {
    override def sql: String = "EDIT"
  }


  def apply(typ: String): MBTSimilarityFunction =
    typ.toLowerCase(Locale.ROOT).replace("_", "") match {
      case "euclid" => EUCLID
      case "l1" => L1
      case "cosine" => COSINE
      case "edit" => EDIT
      case _ =>
        val supported = Seq("euclid")
        throw new IllegalArgumentException(s"Unsupported metric similarity function '$typ'. " +
          "Supported metric similarity functions include: "
          + supported.mkString("'", "', '", "'") + ".")
    }
}


case class MBTSimilarityRangeExpression(similarity: Expression, function: MBTSimilarityFunction,
                                        center: Point[Any], radius: Double)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    val point = MBTSimilarityExpression.getPoints(
      input.asInstanceOf[UnsafeArrayData])
    point.minDist(center, function) <= radius
  }
}

case class MBTDeleteExpression(similarity: Expression, function: MBTSimilarityFunction, center: Point[Any])
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    val point = MBTSimilarityExpression.getPoints(
      input.asInstanceOf[UnsafeArrayData])
    point
  }
}

case class MBTInsertExpression(similarity: Expression, function: MBTSimilarityFunction, center: Point[Any])
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    val point = MBTSimilarityExpression.getPoints(
      input.asInstanceOf[UnsafeArrayData])
    point
  }
}