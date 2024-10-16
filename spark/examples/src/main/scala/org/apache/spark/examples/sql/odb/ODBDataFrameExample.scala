///*
// *  Copyright 2023 by DIMS Project
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// */
//
//package org.apache.spark.examples.sql.odb
//
//import org.apache.spark.internal.Logging
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.expressions.odb.ODBSimilarityFunction
//import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
//import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
//
//object ODBDataFrameExample extends Logging {
//
//  case class MetricRecord(id: Long, metric: Array[Double])
//
//  case class MetricRecordString(id: Long, metric: String)
//
//  private def getMetric(line: (String, Long)): MetricRecord = {
//    val points = line._1.split(" ").map(x => x.toDouble)
//    MetricRecord(line._2, points)
//  }
//
//  private def getMetricString(line: (String, Long)): MetricRecordString = {
//    MetricRecordString(line._2, line._1)
//  }
//
//  def main(args: Array[String]) {
//    val spark = SparkSession
//      .builder()
//      //      .master("spark://node20:7077")
//      .master("local[*]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .getOrCreate()
//
//    // For implicit conversions like converting RDDs to DataFrames
//    import spark.implicits._
//
//    val textDir = if (args.length > 0) args(0) else "examples/src/main/resources/mpeg.txt"
//    ODBConfigConstants.DATA_TYPE = if (args.length > 1) args(1).toInt else 0
//    ODBConfigConstants.ODB_DISTANCE_TYPE = if (args.length > 2) args(2).toInt else 1
//    ODBConfigConstants.ODB_MODE = if (args.length > 3) args(3).toInt else 3
//    ODBConfigConstants.KNN = if (args.length > 4) args(4).toInt else 8
//    ODBConfigConstants.RANGE = if (args.length > 5) args(5).toDouble else 3800
//    ODBConfigConstants.LOCAL_M_TREE_INNER_ORDER = if (args.length > 6) args(6).toInt else 3
//    ODBConfigConstants.LOCAL_M_TREE_LEAF_ORDER = if (args.length > 7) args(7).toInt else 30
//    ODBConfigConstants.BPlusTreeOrder = if (args.length > 8) args(8).toInt else 30
//    ODBConfigConstants.GlobalBPlusTreeOrder = if (args.length > 9) args(9).toInt else 3
//    val SEARCH_TIME = if (args.length > 10) args(10).toInt else 3
//    val INSERT_OR_NOT = if (args.length > 11) args(11).toInt else 1
//    val INSERT_NUM = if (args.length > 12) args(12).toInt else 10
//
//    val distanceFunction = ODBConfigConstants.ODB_DISTANCE_TYPE match {
//      case 0 => ODBSimilarityFunction.EUCLID
//      case 1 => ODBSimilarityFunction.L1
//      case 2 => ODBSimilarityFunction.COSINE
//      case 3 => ODBSimilarityFunction.EDIT
//    }
//
//    val metric = spark.sparkContext
//      .textFile(textDir)
//      .zipWithIndex()
//      .filter(_._1 != "")
//
//    if (ODBConfigConstants.DATA_TYPE == 0) {
//
//      val metricData = metric.map(getMetric)
//      val df1 = metricData.toDF()
//      df1.createOrReplaceTempView("metric1")
//      df1.createODBIndex(distanceFunction, df1("metric"), "metric_index1")
//
//      if (INSERT_OR_NOT == 1) {
//        for (i <- 0 until SEARCH_TIME) {
//          val radius = ODBConfigConstants.RANGE
//          val queryPoint = Point[Any](metricData.filter(t => t.id == (982 + i)).take(1).head.metric)
//          df1.odbSimilarityWithRangeSearch(queryPoint, radius, df1("metric"), distanceFunction).show()
//        }
//        for (i <- 0 until INSERT_NUM) {
//          val insertPoint = Point[Any](metricData.takeSample(false, 1).take(1).head.metric)
//          df1.odbInsert(insertPoint, df1("metric"), distanceFunction).show()
//        }
//        for (i <- 0 until INSERT_NUM) {
//          val deletePoint = Point[Any](metricData.takeSample(false, 1).take(1).head.metric)
//          df1.odbDelete(deletePoint, df1("metric"), distanceFunction).show()
//        }
//      }
//      else {
//        for (i <- 0 until SEARCH_TIME) {
//          val nn = ODBConfigConstants.KNN
//          val queryPoint = Point[Any](metricData.filter(t => t.id == (982 + i)).take(1).head.metric)
//          df1.odbSimilarityWithKNNSearch(queryPoint, df1("metric"),
//            distanceFunction, nn).show()
//        }
//      }
//      for (i <- 0 until SEARCH_TIME) {
//        val radius = ODBConfigConstants.RANGE
//        val queryPoint = Point[Any](metricData.filter(t => t.id == (982 + i)).take(1).head.metric)
//        df1.odbSimilarityWithRangeSearch(queryPoint, radius, df1("metric"), distanceFunction).show()
//      }
//    }
//
//    else {
//      val metricData = metric.map(getMetricString)
//      val df1 = metricData.toDF()
//      df1.createOrReplaceTempView("metric1")
//      df1.createODBIndex(distanceFunction, df1("metric"), "metric_index1")
//      if (INSERT_OR_NOT == 1) {
//        for (i <- 0 until SEARCH_TIME) {
//          val radius = ODBConfigConstants.RANGE
//          val queryPoint = Point[Any](metricData.filter(t => t.id == (982 + i)).take(1).head.metric)
//          df1.odbSimilarityWithRangeSearch(queryPoint, radius, df1("metric"), distanceFunction).show()
//        }
//        for (i <- 0 until INSERT_NUM) {
//          val insertPoint = Point[Any](metricData.takeSample(false, 1).take(1).head.metric)
//          df1.odbInsert(insertPoint, df1("metric"), distanceFunction).show()
//        }
//        for (i <- 0 until INSERT_NUM) {
//          val deletePoint = Point[Any](metricData.takeSample(false, 1).take(1).head.metric)
//          df1.odbDelete(deletePoint, df1("metric"), distanceFunction).show()
//        }
//      } else {
//        for (i <- 0 until SEARCH_TIME) {
//          val nn = ODBConfigConstants.KNN
//          val queryPoint = Point[Any](metricData.filter(t => t.id == (982 + i)).take(1).head.metric)
//          df1.odbSimilarityWithKNNSearch(queryPoint, df1("metric"),
//            distanceFunction, nn).show()
//        }
//      }
//      for (i <- 0 until SEARCH_TIME) {
//        val radius = ODBConfigConstants.RANGE
//        val queryPoint = Point[Any](metricData.filter(t => t.id == (982 + i)).take(1).head.metric)
//        df1.odbSimilarityWithRangeSearch(queryPoint, radius, df1("metric"), distanceFunction).show()
//      }
//    }
//    spark.stop()
//  }
//}
