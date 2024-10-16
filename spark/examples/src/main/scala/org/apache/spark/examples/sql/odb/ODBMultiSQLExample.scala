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
//    df1.createODBIndex(df1("metricDouble"), df1("metricString"), df1("metricM"), df1("metricMaxDis"), "metric_index1")

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
          //          val queryMetricStr = "ODBPOINT(1.0;580.53;7.29;0.96;11.34;1.651;'Plant-based,foods,and,beverages';-0.203939,0.047418,0.114095,0.35395,-0.128798,0.145616,0.070353,-0.11815,-0.090278,0.046224,0.050086,-0.107897,-0.013373,0.11518,-0.199978,0.06288,-0.141927,0.023244,0.073079,-0.093886,0.123237,-0.070354,0.093628,0.006863,-0.101345,-0.012614,-0.124607,0.105347,-0.020684,0.022766,-0.125317,-0.077355,0.103841,0.075819,-0.133708,0.024577,0.176948,-0.049615,0.064263,0.13208,-0.049072,-0.078016,0.074246,0.16314,-0.125895,-0.203966,-0.059272,0.139154,-0.009345,0.078342,-0.042996,-0.05447,-0.065782,-0.052694,0.00982,-0.126899,0.053786,-0.125644,-0.116014,-0.087796,-0.129645,0.092383,-0.120945,0.017361,-0.080644,-0.186768,-0.182726,0.004544,0.087264,-0.036567,0.18131,-0.052979,-0.030775,0.007867,-0.101128,-0.273546,-0.329536,-0.043972,0.011224,-0.148248,-0.019884,-0.11929,0.153225,0.004839,-0.002116,-0.1148,-0.13406,0.272407,-0.003682,-0.111274,-0.160251,-0.217773,-0.055935,-0.003645,-0.027832,-0.093309,0.011882,0.049622,0.158854,-0.113932;49.0,17.0,16.0,16.0,24.0,8.0,27.0,16.0,18.0,31.0,14.0,16.0,170.0,135.0,235.0,210.0,211.0,196.0,210.0,215.0,209.0,197.0,184.0,181.0,205.0,191.0,193.0,175.0,147.0,178.0,170.0,188.0,175.0,165.0,153.0,156.0,163.0,170.0,173.0,154.0,148.0,143.0,162.0,170.0,242.0,206.0,209.0,198.0,210.0,211.0,197.0,200.0,182.0,182.0,201.0,181.0,193.0,172.0,130.0,167.0,156.0,185.0,171.0,147.0,156.0,146.0,160.0,174.0,167.0,133.0,126.0,135.0,132.0,157.0,8.0,15.0,2.0,2.0,3.0,15.0,15.0,12.0,3.0,4.0,2.0,14.0,13.0,10.0,9.0,6.0,4.0,12.0,8.0,7.0,10.0,7.0,5.0,12.0,12.0,7.0,9.0,7.0,4.0,10.0,11.0,11.0,8.0,6.0,2.0,2.0,6.0,1.0,4.0,2.0,1.0,6.0,3.0,4.0,1.0,1.0,7.0,3.0,3.0,2.0,3.0,5.0,1.0,6.0,3.0,3.0,3.0,4.0,4.0,6.0,3.0,4.0,5.0,5.0,5.0,2.0,6.0,5.0,5.0,5.0,5.0,5.0,3.0,4.0,3.0,4.0,3.0,2.0,7.0,6.0,4.0,5.0,5.0,5.0,4.0,2.0,6.0,4.0,4.0,5.0,4.0,4.0,5.0,2.0,5.0,1.0,7.0,4.0,5.0,4.0,3.0,6.0,5.0,2.0,4.0,1.0,7.0,3.0,3.0,3.0,6.0,4.0,1.0,1.0,1.0),METRICM(0,0,0,0,0,0,6,5,4),QUERYM(1,0,0,0,0,0,0,1,1),0.294)"
          //          println(queryMetricStr)
          //                val aaa = s"SELECT * FROM metric1 WHERE metric1.metricDouble IN ODBRANGE" + s"(" + queryMetricStr + ")"
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
          //          val aaa = s"SELECT * FROM metric1 WHERE metric1.metric ODBKNN" + s"(" + queryMetricStr + s",$k)"
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
