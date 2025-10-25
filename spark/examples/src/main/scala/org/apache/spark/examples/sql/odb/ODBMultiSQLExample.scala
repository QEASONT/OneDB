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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.execution.odb.util.MetricRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point


import scala.collection.mutable

// scalastyle:off println
object ODBMultiSQLExample {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._


    val sc = spark.sparkContext
    val fileData = "examples/src/main/resources/dfood.txt"
    val fileContents = sc.textFile(fileData)

    val headerInfo = fileContents.take(3)
    // read num of rows and columns
    val n = headerInfo(0).split(" ")(0).toInt
    val m = headerInfo(0).split(" ")(1).toInt

    // read metricm and metricMaxDis
    val metricM = headerInfo(1).split(" ").map(x => x.toInt)
    val metricMaxDis = headerInfo(2).split(" |\t").map(x => x.toDouble)
    val metricData = fileContents.zipWithIndex().filter(_._2 >= 3).map(MetricRecord.getMetric(_, metricM, metricMaxDis))
    //    val metricMeanDis = MetricRecord.calculateMetricMeanDis(metricData, metricM, metricMaxDis)

    val queryData = "examples/src/main/resources/dfood_q.txt"
    val queryContents = sc.textFile(queryData)
    val queryHeaderInfo = queryContents.take(4)
    val queryM = queryHeaderInfo(0).split(" ").map(x => x.toInt)
    val queryKVal = queryHeaderInfo(1).split(" |\t").zipWithIndex.filter(_._2 > 0).map(x => x._1.toInt)
    val queryRad = queryHeaderInfo(3).split(" |\t").map(x => x.toDouble)
    val queryPos = queryContents.zipWithIndex().filter(_._2 >= 4).map(x => x._1.toInt).collect()
    //
    val df1 = metricData.toDF()
    df1.createOrReplaceTempView("metric1")


    for (m <- queryPos.indices) {
      for (i <- queryRad.indices) {
        val radius = queryRad(i)
        val filterQueryP = metricData.filter(t => t.id == queryPos(m))
        if (filterQueryP.count() != 0) {
          val queryP = filterQueryP.take(1).head
          val pintsArray: Array[Point[Any]] = metricM.zipWithIndex.map(x =>
            x._1 match {
              case 0 | 1 | 2 | 3 | 4 | 5 =>
                Point[Any](queryP.metricDouble(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
              case 6 =>
                Point[Any](queryP.metricString(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
            })
          val searchMetric = MetricData(pintsArray)
          val queryMetricStr = "ODBPOINT(" + searchMetric.points.map(point =>
            s"${point.toString}").mkString("|") + s"),METRICM(${metricM.mkString(",")}),QUERYM(${queryM.mkString(",")})" + "," + radius
          val res = spark.sql(s"SELECT * FROM metric1 WHERE metric1.metricDouble IN ODBRANGE" + s"(" + queryMetricStr + ")")
          res.map(
            attr => {
              val id = attr.getAs[Long](0)
              val metricDouble = attr.getAs[mutable.WrappedArray[mutable.WrappedArray[Double]]](1).map(
                x => if (x == null) null else x.toArray).toArray
              val metricString = attr.getAs[mutable.WrappedArray[String]](2).map(
                x => if (x == null) null else x).toArray
              val metricRecord = MetricRecord(id, metricDouble, metricString, metricM, metricMaxDis)
              (id, metricRecord.toString)
            }
          ).toDF("ID", "MetricRecord").show(20, false)
        }
      }
      for (i <- queryKVal.indices) {
        val k = queryKVal(i)
        val filterQueryP = metricData.filter(t => t.id == queryPos(m))
        if (filterQueryP.count() != 0) {
          val queryP = filterQueryP.take(1).head
          val pintsArray: Array[Point[Any]] = metricM.zipWithIndex.map(x =>
            x._1 match {
              case 0 | 1 | 2 | 3 | 4 | 5 =>
                Point[Any](queryP.metricDouble(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
              case 6 =>
                Point[Any](queryP.metricString(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
            })
          val searchMetric = MetricData(pintsArray)
          val queryMetricStr = "ODBPOINT(" + searchMetric.points.map(point =>
            s"${point.toString}").mkString("|") + s"),METRICM(${metricM.mkString(",")}),QUERYM(${queryM.mkString(",")})" + "," + k
          println(queryMetricStr)
          spark.sql(s"SELECT * FROM metric1 WHERE metric1.metricDouble IN ODBKNN"
            + s"(" + queryMetricStr + ")").map(
            attr => {
              val id = attr.getAs[Long](0)
              val metricDouble = attr.getAs[mutable.WrappedArray[mutable.WrappedArray[Double]]](1).map(
                x => if (x == null) null else x.toArray).toArray
              val metricString = attr.getAs[mutable.WrappedArray[String]](2).map(
                x => if (x == null) null else x).toArray
              val metricRecord = MetricRecord(id, metricDouble, metricString, metricM, metricMaxDis)
              (id, metricRecord.toString)
            }
          ).toDF("ID", "MetricRecord").show(20, false)
        }
      }
    }

    spark.stop()
  }
}
