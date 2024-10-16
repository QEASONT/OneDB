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

import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.execution.odb.util.{MetricDouble, MetricString, Metrics}

import scala.collection.immutable.ListMap

object AggregationModel {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("AggregationFunctionModel")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // Load Data
    val schema = StructType(Array(
      StructField("base_modal1", VectorType, true),
      StructField("base_modal2", VectorType, true),
      StructField("base_modal3", VectorType, true),
      StructField("base_modal4", VectorType, true),
      StructField("query_modal1", VectorType, true),
      StructField("query_modal2", VectorType, true),
      StructField("query_modal3", VectorType, true),
      StructField("query_modal4", VectorType, true),
      StructField("gt", ArrayType(IntegerType), true)
    ))


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


    val searchPoint = metricDataRDD.sample(false, 0.0006)
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
        val weightDist = originDist.zip(weight).map(y => y._1 * y._2)

        // searchPointID, otherPointID, originDist, weightDist
        (x._1, x._2, originDist, weightDist, weightDist.sum)
      }
    )
    // find min weightDist in each searchPointID
    // (x._1, x._2, originDist, weightDist, gt or not)
    val dataa = pointPairRDDWithWeight.groupBy(_._1).flatMap(
      x => {
        // x._2 -> iterable
        val res = x._2.filter(y => y._1 != y._2).toArray.sortBy(_._5).zipWithIndex.map {
          case (value, index) => if (index < 100) (value._1, value._2, value._3, value._4, value._5, 1) else (value._1, value._2, value._3, value._4, value._5, 0)
        }
        res
      }
    ).zipWithIndex().map(x => {
      (x._2, x._1._1, x._1._2, x._1._3, x._1._4, x._1._5, x._1._6)
    })

    val data = dataa.toDF("id", "searchPointID", "otherPointID", "originDist", "weightDist", "aggregation", "gt")


    //    val data = spark.read.schema(schema).json("path/to/data.json")
    // Define the Aggregation Function Model
    class AggregationFunctionModel(override val uid: String) extends Model[AggregationFunctionModel] with Params {
      final val omega = new Param[Array[Double]](this, "omega", "Weight Array for all modals")

      def setOmega(value: Array[Double]): this.type = set(omega, value)

      def getOmega: Array[Double] = $(omega)

      override def copy(extra: ParamMap): AggregationFunctionModel = defaultCopy(extra)

      override def transform(dataset: Dataset[_]): DataFrame = {
        val weighted = udf((distances: Seq[Double]) => {
          val weights = getOmega
          distances.zip(weights).map { case (distance, weight) =>
            distance * weight
          }
        })
        val weightedSum = udf((distances: Seq[Double]) => {
          distances.sum
        })
        //        println("omega: " + getOmega.mkString(", "))
        val res = dataset.withColumn("weightDist", weighted(col("originDist"))).withColumn("aggregation", weightedSum(col("weightDist")))
        //        val cc = res.take(1).head
        res
      }

      override def transformSchema(schema: StructType): StructType = {
        schema.add(StructField("aggregation", DoubleType, false))
      }
    }

    // Define Loss and Optimizer
    def aggregationFunctionLoss(phi_positive: DataFrame, phi_negative: DataFrame): DataFrame = {


      val t = 1
      // rename the columns
      val phi_negative_rename = phi_negative.withColumnRenamed("weightDist", "weightDistNeg").withColumnRenamed("aggregation", "aggregationNeg")
      val lossGradient = phi_positive.join(
        phi_negative_rename,
        "id"
      ).flatMap { row =>
        val searchPointID = row.getAs[Long]("searchPointID")
        val posWeightDist = row.getAs[Seq[Double]]("weightDist").toArray
        val posAgg = row.getAs[Double]("aggregation")
        val negWeightDist = row.getAs[Seq[Double]]("weightDistNeg").toArray
        val negAgg = row.getAs[Double]("aggregationNeg")

        // 计算正样本和负样本的 exp 值
        val posExp = math.exp(posAgg / t)
        val negExp = math.exp(negAgg / t)
        //        val negExpSum = negWeightDist.map(dist => math.exp(dist / t)).sum

        // 计算损失值
        val loss = -math.log(posExp / (posExp + negExp))

        // 计算每个权重的梯度
        val denom = posExp + negExp
        val gradients = posWeightDist.indices.map { i =>
          val dL_dWi = -(posWeightDist(i) - negWeightDist(i)) / denom
          dL_dWi
        }

        Seq((searchPointID, loss, gradients))
      }.toDF("searchPointID", "loss", "gradient")

      lossGradient

    }


    // 自定义优化器
    class AggregationFunctionOptimizer(var omega: Array[Double], val lr: Double) {

      def step(gradients: Array[Double]): Unit = {

        val random_num = 0.4
        omega = omega.zip(gradients).map { case (weight, grad) =>
          weight - lr * random_num * grad
        }
      }

      def getOmega: Array[Double] = omega
    }


    // Example usage
    var model = new AggregationFunctionModel("aggModel")
      .setOmega(Array.fill(m)(0.5))


    // Placeholder for phi_positive and phi_negative computation
    val phi_positive_ids = data.filter($"gt" === 1).select("id").collect().map(_.getLong(0))
    //("id", "searchPointID", "otherPointID", "originDist", "weightDist", "aggregation", "gt")
    val phi_positive_df = data.filter(col("id").isin(phi_positive_ids: _*)).rdd.zipWithIndex().map(x => {
      (x._2, x._1.getLong(1), x._1.getLong(2), x._1.getSeq[Double](3), x._1.getSeq[Double](4), x._1.getDouble(5), x._1.getInt(6))
    }).toDF("id", "searchPointID", "otherPointID", "originDist", "weightDist", "aggregation", "gt")

    def evalRecallAtK(phi_positive: DataFrame, phi_negative: DataFrame): Double = {
      val pos_id = phi_positive.select("searchPointID", "otherPointID").collect()
      val neg_id = phi_negative.select("searchPointID", "otherPointID").collect()
      val pos_id_set = pos_id.map(x => (x.getLong(0), x.getLong(1))).toSet
      val neg_id_set = neg_id.map(x => (x.getLong(0), x.getLong(1))).toSet
      val pos_id_count = pos_id_set.size
      val neg_id_count = neg_id_set.size
      val tp = pos_id_set.intersect(neg_id_set).size
      val recall = tp.toDouble / pos_id_count
      recall
    }

    val optimizer = new AggregationFunctionOptimizer(model.getOmega, lr = 10)
    var minLoss = Double.MaxValue
    // Placeholder for optimization loop
    for (epoch <- 0 until 1000) {
      println(s"Epoch: #$epoch")

      // find all different searchPointID
      //      val phi_negative_ids = data.select("id").distinct().collect().map(_.getLong(0))
      val phi_negative_df = model.transform(data)

      //      phi_negative_df.show()
      //      phi_negative_df.printSchema()
      //      val windowSpec = Window.partitionBy("searchPointID").orderBy("weightDist")
      //      val dfWithRowNumber = phi_negative_df.withColumn("row_number", row_number().over(windowSpec))
      // 过滤出每个searchPointID前100个aggregation值最小的行
      //      "id", "searchPointID", "otherPointID", "originDist", "weightDist", "aggregation", "gt"

      val top100AggregationDF = phi_negative_df.rdd.groupBy(_.getLong(1)).flatMap(x => {
        x._2.toArray.sortBy(_.getDouble(5)).take(100)
      }).zipWithIndex().map(x => {
        (x._2, x._1.getLong(1), x._1.getLong(2), x._1.getSeq[Double](3), x._1.getSeq[Double](4), x._1.getDouble(5))
      }).toDF("id", "searchPointID", "otherPointID", "originDist", "weightDist", "aggregation")
      // print dataframe
      //      top100AggregationDF.show()

      //      val loss = aggregationFunctionLoss(phi_positive_df, top100AggregationDF)


      val result = aggregationFunctionLoss(phi_positive_df, top100AggregationDF)

      // calculate average gradients
      //      val gradients = result.groupBy("searchPointID").agg(
      //        avg("loss").as("avg_loss"),
      //        collect_list("gradient").as("gradients")
      //      ).select("gradients").collect().map(_.getSeq[Seq[Double]](0).map(_.toArray).reduce((a, b) => a.zip(b).map { case (x, y) => x + y }).map(_ / 100))
      // calculate average gradients

      // Compute gradients (this is a placeholder, actual gradient computation depends on your model and loss function)
      //      result.show()
      val collectedResult = result.select("searchPointID", "loss", "gradient").collect().map(row => (row.getLong(0), row.getDouble(1), row.getSeq[Double](2).toArray))
      // 计算平均损失
      val avgLoss = collectedResult.map(_._2).sum / collectedResult.length

      // 提取所有梯度
      val gradients = collectedResult.map(_._3)

      // 计算每个位置上的均值
      val avgGradient = gradients.transpose.map(column => column.sum / column.length)

      // 显示平均损失和平均梯度
      val rec = evalRecallAtK(phi_positive_df, top100AggregationDF)
      println(s"Average Loss: $avgLoss , Recall at 100: $rec")
      //      println(s"Average Gradient: ${avgGradient.mkString(", ")}")
      //      println(s"True weights: omega=${generateWeight.mkString(", ")} ")
      //      println(s"Trained model parameters: omega=${model.getOmega.mkString(", ")}")
      optimizer.step(avgGradient)

      if (math.abs(avgLoss) < minLoss) {
        minLoss = math.abs(avgLoss)
      }
      if (minLoss < 0.1) {
        return
      }
      model = model.setOmega(optimizer.getOmega)


    }
    println(s"True weights: omega=${generateWeight.mkString(", ")} ")
    println(s"Trained model parameters: omega=${model.getOmega.mkString(", ")}")
    spark.stop()
  }
}
