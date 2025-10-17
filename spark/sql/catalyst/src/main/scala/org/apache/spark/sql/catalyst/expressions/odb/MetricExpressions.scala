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

package org.apache.spark.sql.catalyst.expressions.odb

import org.apache.commons.math3.util.DoubleArray
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, GenericInternalRow, UnaryExpression, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.{MetricData, ODBSimilarity}
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType}

import java.util.Locale

case class ODBSimilarityExpression(function: ODBSimilarityFunction,
                                   met1: Expression, met2: Expression)
  extends BinaryExpression with CodegenFallback {

  override def left: Expression = met1

  override def right: Expression = met2

  override def dataType: DataType = DoubleType

  override def nullSafeEval(m1: Any, m2: Any): Any = {
    val metric1 = m1 match {
      case t: Point[Any] => t
      case uad: UnsafeRow => ODBSimilarityExpression.getPoints(uad)
    }
    val metric2 = m2 match {
      case t: Point[Any] => t
      case uad: UnsafeRow => ODBSimilarityExpression.getPoints(uad)
    }
    ODBSimilarity.EUCLIDistance.evalWithPoint(metric1, metric2)
  }
}

object ODBSimilarityExpression {
  def getPoints(rawData: UnsafeRow): Array[Point[Any]] = {
    val id = rawData.getLong(0)
    val doubleAAUnsafe = rawData.getArray(1)
    val stringArray = rawData.getArray(2).toObjectArray(StringType)
    val metricMArray = rawData.getArray(3).toIntArray
    val metricMaxDis = rawData.getArray(4).toDoubleArray
    val doubleAA = new Array[Array[Double]](doubleAAUnsafe.numElements())
    val pointArray = doubleAA.zipWithIndex.map(x => {
      val index = x._2
      //      val aaa = doubleAAUnsafe.getArray(6)
      if (doubleAAUnsafe.getArray(index) != null) {
        doubleAA(index) = doubleAAUnsafe.getArray(index).toDoubleArray
      }
      val point = metricMArray(index) match {
        case 0 | 1 | 2 | 3 | 4 | 5 =>
          Point[Any](doubleAA(index), index, metricMArray(index), metricMaxDis(index), id)
        case 6 =>
          Point[Any](stringArray(index).toString, index, metricMArray(index), metricMaxDis(index), id)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported metric type '${metricMArray(index)}'. " +
            "Supported metric types include: "
            + "0, 1, 2, 3, 4, 5, 6.")
      }
      point
    })
    pointArray
  }

}

case class ODBSimilarityWithKNNExpression(similarity: Expression, centers: MetricData,
                                          count: Int, queryM: Array[Int])
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    throw new NotImplementedError()
  }

}

sealed abstract class ODBSimilarityFunction extends Serializable {
  def sql: String
}

object ODBSimilarityFunction {
  case object EUCLID extends ODBSimilarityFunction {
    override def sql: String = "EUCLID"
  }

  case object L1 extends ODBSimilarityFunction {
    override def sql: String = "L1"
  }

  case object COSINE extends ODBSimilarityFunction {
    override def sql: String = "COSINE"
  }

  case object EDIT extends ODBSimilarityFunction {
    override def sql: String = "EDIT"
  }

  case object TIME extends ODBSimilarityFunction {
    override def sql: String = "TIME"
  }

  case object CUSTOM extends ODBSimilarityFunction {
    override def sql: String = "CUSTOM"
  }

  def apply(typ: String): ODBSimilarityFunction =
    typ.toLowerCase(Locale.ROOT).replace("_", "") match {
      case "euclid" => EUCLID
      case "l1" => L1
      case "cosine" => COSINE
      case "edit" => EDIT
      case "time" => TIME
      case "custom" => CUSTOM
      case _ =>
        val supported = Seq("euclid")
        throw new IllegalArgumentException(s"Unsupported metric similarity function '$typ'. " +
          "Supported metric similarity functions include: "
          + supported.mkString("'", "', '", "'") + ".")
    }
}


case class ODBSimilarityRangeExpression(similarity: Expression,
                                        centers: MetricData, radius: Double, queryM: Array[Int])
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {

    val len = input.asInstanceOf[GenericInternalRow].values.length
    if (len != 3) {
      throw new IllegalArgumentException(s"Unsupported input length '$len'. " +
        "Supported input length include: "
        + "2.")
    }
    val doubleRow = input.asInstanceOf[GenericInternalRow].getArray(0).asInstanceOf[UnsafeArrayData]
    val stringRow = input.asInstanceOf[GenericInternalRow].getArray(1).asInstanceOf[UnsafeArrayData]
    val maxDisRow = input.asInstanceOf[GenericInternalRow].getArray(2).asInstanceOf[UnsafeArrayData].toDoubleArray
    //    val aaa = row.getArray(0).toDoubleArray

    val metricM = centers.points.map(x => x.metricValue)
    val res = new Array[Array[Double]](metricM.length)
    //    val bbb = doubleRow.getArray(0).toDoubleArray
    //    val aaa = stringRow.toObjectArray(StringType)


    val pointArray = res.zipWithIndex.map(x => {
      val index = x._2
      metricM(index) match {
        case 0 | 1 | 2 | 3 | 4 | 5 =>
          Point[Any](doubleRow.getArray(index).toDoubleArray, index, metricM(index), maxDisRow(index))
        // row.getArray(index).toDoubleArray
        case 6 =>
          Point[Any](stringRow.toObjectArray(StringType), index, metricM(index), maxDisRow(index))
        // x._1
        // row.getArray(index).toObjectArray(StringType)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported metric type '${metricM(index)}'. " +
            "Supported metric types include: "
            + "0, 1, 2, 3, 4, 5, 6.")
      }
    })

    //    null
    pointArray.zip(centers.points).zipWithIndex.map(x => {
      val point = x._1._1
      val center = x._1._2
      point.minDist(center) * queryM(x._2)
    }).sum <= radius
  }
}

case class ODBDeleteExpression(similarity: Expression, center: MetricData)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    val point = ODBSimilarityExpression.getPoints(
      input.asInstanceOf[UnsafeRow])
    point
  }
}

case class ODBInsertExpression(similarity: Expression, center: MetricData)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    val point = ODBSimilarityExpression.getPoints(
      input.asInstanceOf[UnsafeRow])
    point
  }
}