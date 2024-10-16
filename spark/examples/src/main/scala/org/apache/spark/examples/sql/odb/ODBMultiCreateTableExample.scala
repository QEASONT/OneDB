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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.execution.odb.util.MetricRecord

import scala.collection.mutable

object ODBMultiCreateTableExample extends Logging {


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      //      .master("spark://node20:7077")
      .master("local[*]")
      .config("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer").config("hive.metastore.uris", "thrift://10.214.151.180:9083")
      .enableHiveSupport()
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val sc = spark.sparkContext
    // read from file
    //    spark.sql("show databases").show()
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

    val df1 = metricData.toDF()
    df1.createOrReplaceGlobalTempView("metric1")
    df1.createODBIndex(df1("metricDouble"), df1("metricString"), df1("metricM"), df1("metricMaxDis"), "metric_index1")
    spark.sql("use qtspark")
    //        spark.sql("create table multiMetric")
    df1.write.mode("overwrite").saveAsTable("multiMetric")

    // Create a Dataframe.
//    val df = Seq((1, "John"), (2, "Jane"), (3, "Bob")).toDF("id", "name")
//
//    // Save DataFrame into a table
//    df.write.saveAsTable("qtspark.multiMetric")
    spark.stop()
  }
}
