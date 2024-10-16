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

package org.apache.spark.examples.sql.odb

import org.apache.spark.internal.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.execution.odb.util.{MetricDouble, MetricString, Metrics}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.immutable.ListMap

object ODBKNNWeightLearning extends Logging {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      //      .master("spark://node20:7077")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    // read from file
    val fileData = "examples/src/main/resources/dfood.txt"
    val fileContents = sc.textFile(fileData)

    val headerInfo = fileContents.take(3)
    // read num of rows and columns
    val n = headerInfo(0).split(" ")(0).toInt
    val m = headerInfo(0).split(" ")(1).toInt


    // read metricm and metricMaxDis
    val metricM = headerInfo(1).split(" ").map(x => x.toInt)
    val metricMaxDis = headerInfo(2).split(" |\t").map(x => x.toDouble)
    val metricData = fileContents.zipWithIndex().filter(_._2 >= 3).map(Metrics.getMetric(_, metricM, metricMaxDis))

    // Determine all metric types and their positions
    val metricTypes = ListMap(metricData.take(1).head.metrics.zipWithIndex.map {
      case (MetricDouble(_), idx) => s"metricDouble_$idx" -> ArrayType(DoubleType)
      case (MetricString(_), idx) => s"metricString_$idx" -> StringType
    }: _*)

    // Define the base schema
    val baseSchema = Seq(
      StructField("id", LongType, nullable = false),
      StructField("metricM", ArrayType(IntegerType), nullable = true),
      StructField("metricMaxDis", ArrayType(DoubleType), nullable = true)
    )

    // Create the dynamic schema
    val dynamicSchema = StructType(baseSchema ++ metricTypes.map { case (name, dataType) => StructField(name, dataType, nullable = true) }.toSeq)

    // Convert RDD to DataFrame
    val rowRDD = metricData.map(f = record => {
      val metricsMap = record.metrics.zipWithIndex.map {
        case (MetricDouble(v), idx) => s"metricDouble_$idx" -> v
        case (MetricString(v), idx) => s"metricString_$idx" -> v
      }


      val rowValues = Seq(
        record.id.asInstanceOf[Any],
        record.metricM.asInstanceOf[Any],
        record.metricMaxDis.asInstanceOf[Any]
      ) ++ metricsMap.map { case (_, v) => v.asInstanceOf[Any] }

      Row.fromSeq(rowValues)
    })
    val tmp = rowRDD.collect()
    //    val metricMeanDis = MetricRecord.calculateMetricMeanDis(metricData, metricM, metricMaxDis)

    val df = spark.createDataFrame(rowRDD, dynamicSchema)

    // Show the DataFrame
    df.show()

    val metricDataRDD = metricData.zipWithIndex().map(x => {
      val metricM = x._1.metricM
      val pointsArray = metricM.zipWithIndex.map(y => {
        val index = y._2
        val value = y._1
        value match {
          case 0 | 1 | 2 | 3 | 5 => Point[Any](x._1.metrics(index).asInstanceOf[MetricDouble].value, index, value, metricMaxDis(index), x._2) // Double
          case 4 => Point[Any](x._1.metrics(index).asInstanceOf[MetricDouble].value, index, value, metricMaxDis(index), x._2) // Actually Int Array
          case 6 => Point[Any](x._1.metrics(index).asInstanceOf[MetricString].value, index, value, metricMaxDis(index), x._2) // String Array
        }
      })
      MetricData(pointsArray)
    })

    val searchPoint = metricDataRDD.sample(false, 0.003)
    val searchPointCount = searchPoint.count()
    val pointPair = searchPoint.cartesian(metricDataRDD)
    val pointPairCount = pointPair.count()


    // searchPointID, otherPointID, (searchPoint, otherPoint), originDist
    val pointPairRDD = pointPair.map(x => {
      (x._1.id, x._2.id, x, x._1.originDist(x._2))
    })

    val generateWeight = Array.fill(metricM.length)(Math.random())

    val pointPairRDDWithWeight = pointPairRDD.map(
      x => {
        val weight = generateWeight
        val originDist = x._4
        val weightDist = originDist.zip(weight).map(y => y._1 * y._2).sum
        // searchPointID, otherPointID, originDist, weightDist
        (x._1, x._2, originDist, weightDist)
      }
    )
    // find min weightDist in each searchPointID
    val minDistPair = pointPairRDDWithWeight.groupBy(_._1).map(x => x._2.map(y => (y._1, y._2, y._3, y._4)).filter(x => x._1 != x._2).minBy(_._4)).collect()
    val minDistIDPair = minDistPair.map(x => (x._1, x._2))
    val data = pointPairRDDWithWeight.map(x => if (minDistIDPair.contains((x._1, x._2))) (x._1, x._2, x._3, x._4, 1) else (x._1, x._2, x._3, x._4, 0)).groupBy(_._1)


    // 定义计算距离的函数
    def computeDistance(originDist: Array[Double], weights: DenseVector): Double = {
      val w = weights.toArray
      originDist.zip(w).map { case (d, w) => d * w }.sum
    }

    // 定义对比损失函数
    def contrastiveLoss(anchorPair: Iterable[(Array[Double], Int)], weights: DenseVector): (Double, DenseVector) = {
      val pairs = anchorPair.toArray
      val pos = pairs.filter(_._2 == 1).head
      val neg = pairs.filter(_._2 == 0)
      val posDist = computeDistance(pos._1, weights)
      val negDists = neg.map(n => computeDistance(n._1, weights))
      val negExpSum = negDists.map(d => Math.exp(-d)).sum
      val posExp = Math.exp(-posDist)

      val loss = -Math.log(posExp / (posExp + negExpSum))
      val posGradient = pos._1.map(d => -d / (posExp + negExpSum))
      val negGradients = neg.map(n => n._1.map(d => d * Math.exp(-computeDistance(n._1, weights)) / (posExp + negExpSum)))

      val gradients = posGradient.zip(negGradients.transpose.map(_.sum)).map { case (p, n) => p + n }

      (loss, new DenseVector(gradients))
    }

    //
    // 初始化权重
    var weights = new DenseVector(Array.fill(m)(scala.util.Random.nextDouble()))

    // 优化权重
    val learningRate = 0.01
    val numEpochs = 100

    for (epoch <- 1 to numEpochs) {
      val totalLossGradient = data.map { case (id, iter) =>
        val (loss, gradient) = contrastiveLoss(iter.map(x => (x._3, x._5)), weights)
        (loss, gradient)
        // 计算梯度并更新权重
      }.reduce { (a, b) =>
        (a._1 + b._1, new DenseVector(a._2.toArray.zip(b._2.toArray).map { case (x, y) => x + y }))
      }

      val totalLoss = totalLossGradient._1 / minDistIDPair.length
      val totalGradient = totalLossGradient._2

      // 更新权重
      weights = new DenseVector(weights.toArray.zip(totalGradient.toArray).map { case (w, g) => w - learningRate * g })

      println(s"Epoch $epoch, Loss: $totalLoss")
    }


    spark.stop()

  }
}