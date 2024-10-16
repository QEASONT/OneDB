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
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.execution.odb.util.{MetricDouble, MetricRecord, MetricString, Metrics}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.immutable.ListMap

object ODBRangeWeightLearning extends Logging {

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
    val pointPairRDD = pointPair.map(x => {
      (x, x._1.originDist(x._2))
    })

    //    for (i <- 0 to 1000) {
    val generateWeight = Array.fill(metricM.length)(Math.random())
    // random generate weight from 0 to 1


    val pointPairRDDWithWeight = pointPairRDD.map(
      x => {
        val weight = generateWeight
        val originDist = x._2
        val weightDist = originDist.zip(weight).map(y => y._1 * y._2).sum
        (originDist, weightDist)
      }
    )
    // originDist, generateWeight, rank by weight
    val trainingSet = pointPairRDDWithWeight

    val trainingDF = trainingSet.toDF("originDist", "rank")
    trainingDF.select("originDist").show(false)
    trainingDF.select("rank").show(false)

    val toVector = udf((array: Seq[Double]) => Vectors.dense(array.toArray))

    val Array(trainingDFWithVector, testDFWithVector) = trainingDF.withColumn("originDistVec", toVector($"originDist")).randomSplit(Array(0.8, 0.2))


    val assembler = new VectorAssembler()
      .setInputCols(Array("originDistVec"))
      .setOutputCol("features")


    //    val dataWithFeatures = assembler.transform(trainingDFWithVector)
    //    val testData = assembler.transform(testDFWithVector)

    val lr = new LinearRegression()
      .setMaxIter(100)
      .setRegParam(0.1)
      .setElasticNetParam(0.5)
      .setFeaturesCol("features")
      .setLabelCol("rank")


    //    val lrModel = lr.fit(dataWithFeatures)
    //    val weights = lrModel.coefficients
    //    println(s"Coefficients: ${weights} Intercept: ${lrModel.intercept}")
    //    println(s"true wights: ${generateWeight.mkString(",")}")


    // 构建管道
    val pipeline = new Pipeline().setStages(Array(assembler, lr))
    // 构建参数网格
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.01, 0.05, 0.1, 0.2, 0.5))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
      .addGrid(lr.maxIter, Array(50, 100, 200, 300))
      .build()

    // 创建评估器
    val evaluator = new RegressionEvaluator()
      .setLabelCol("rank")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // 创建交叉验证器
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3) // 设置交叉验证的折数

    // 拟合模型
    val cvModel = cv.fit(trainingDFWithVector)

    // 评估模型
    val predictions = cvModel.transform(testDFWithVector)
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")


    // 获取最佳模型并打印权重和截距
    val bestModel = cvModel.bestModel
    val lrModel = bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages.last.asInstanceOf[org.apache.spark.ml.regression.LinearRegressionModel]

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    val weights = lrModel.coefficients

    // 测试集准备
    val testPoint = metricDataRDD.sample(false, 0.001)
    val testPointPair = testPoint.cartesian(metricDataRDD)
    val testPointPairRDD = testPointPair.map(x => {
      (x, x._1.originDist(x._2))
    })

    // 使用训练出来的权重计算加权距离
    val testPointPairRDDWithWeight = testPointPairRDD.map(x => {
      val originDist = x._2
      val weightDist = originDist.zip(weights.toArray).map(y => y._1 * y._2).sum
      (x._1._2.id, originDist, weightDist)
    })

    // 计算真实权重的加权距离
    val trueResult = testPointPairRDDWithWeight.map(x => {
      val originDist = x._2
      val trueWeightDist = originDist.zip(generateWeight).map(y => y._1 * y._2).sum
      (x._1, trueWeightDist, x._3)
    }).toDF("queryPoint", "trueWeightDist", "calculatedWeightDist")

    // 计算绝对误差
    val tolerance = 1e-1
    val resultsWithError = trueResult.withColumn("absError", abs($"trueWeightDist" - $"calculatedWeightDist"))

    // 统计测试结果
    val correctMatches = resultsWithError.filter($"absError" <= tolerance).count()
    val totalTests = resultsWithError.count()
    val accuracy = correctMatches.toDouble / totalTests

    println(s"Accuracy: $accuracy")
    resultsWithError.show()

    // square root over true weight and calculated weight
    val sq = weights.toArray.zip(generateWeight).map(x => (x._1 - x._2) * (x._1 - x._2)).sum
    val sqRoot = Math.sqrt(sq)
    println(s"Root Mean Squared Error (RMSE) on test data = $sqRoot")
    //    结束SparkSession
    spark.stop()

  }
}