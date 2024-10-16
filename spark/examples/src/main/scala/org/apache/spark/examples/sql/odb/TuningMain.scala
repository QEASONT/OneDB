package org.apache.spark.examples.sql.odb

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.execution.odb.util.MetricRecord
//import org.apache.spark.sql.tuning.{PerformanceReply, PerformanceRequest}

object TuningMain {
  def runSpark(sampleSize: Int,
               globalIndexedPivotCount: Int,
               rtreeGlobalMaxEntriesPerNode: Int,
               rtreeLocalMaxEntriesPerNode: Int,
               rtreeGlobalNumPartitions: Int,
               rtreeLocalNumPartitions: Int): (Int, Long, Long) = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    // Load the metric data
    val fileData = "examples/src/main/resources/dfood.txt"
    val fileContents = sc.textFile(fileData)
    val headerInfo = fileContents.take(3)
    val n = headerInfo(0).split(" ")(0).toInt
    val m = headerInfo(0).split(" ")(1).toInt
    val metricM = headerInfo(1).split(" ").map(x => x.toInt)
    val metricMaxDis = headerInfo(2).split(" |\t").map(x => x.toDouble)
    val metricData = fileContents.zipWithIndex().filter(_._2 >= 3).map(MetricRecord.getMetric(_, metricM, metricMaxDis))


    // Perform range search and KNN search

    ODBConfigConstants.SAMPLE_SIZE = sampleSize
    ODBConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT = globalIndexedPivotCount
    ODBConfigConstants.RTREE_GLOBAL_MAX_ENTRIES_PER_NODE = rtreeGlobalMaxEntriesPerNode
    ODBConfigConstants.RTREE_LOCAL_MAX_ENTRIES_PER_NODE = rtreeLocalMaxEntriesPerNode
    ODBConfigConstants.RTREE_GLOBAL_NUM_PARTITIONS = rtreeGlobalNumPartitions
    ODBConfigConstants.RTREE_LOCAL_NUM_PARTITIONS = rtreeLocalNumPartitions


    val queryData = "examples/src/main/resources/dfood_q.txt"
    val queryContents = sc.textFile(queryData)
    val queryHeaderInfo = queryContents.take(4)
    val queryM = queryHeaderInfo(0).split(" ").map(x => x.toInt)
    val queryKVal = queryHeaderInfo(1).split(" |\t").zipWithIndex.filter(_._2 > 0).map(x => x._1.toInt)
    val queryRad = queryHeaderInfo(3).split(" |\t").map(x => x.toDouble)
    val queryPos = queryContents.zipWithIndex().filter(_._2 >= 4).map(x => x._1.toInt).collect()

    val df1 = metricData.toDF()
    df1.createOrReplaceTempView("metric1")
    //    df1.createODBIndex(df1("metricDouble"), df1("metricString"), df1("metricM"), df1("metricMaxDis"), "metric_index1")
    val rangeQueryTimeArray = new Array[Long](queryPos.length * queryRad.length)
    val knnQueryTimeArray = new Array[Long](queryPos.length * queryKVal.length)
    var startTime = System.currentTimeMillis()

    for (m <- queryPos.indices) {
      for (i <- queryRad.indices) {
        startTime = System.currentTimeMillis()
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
          df1.odbSimilarityWithRangeSearch(searchMetric, radius, queryM, df1("metricDouble")).show()
        }
        rangeQueryTimeArray(m * queryRad.length + i) = System.currentTimeMillis() - startTime
      }
      for (i <- queryKVal.indices) {
        startTime = System.currentTimeMillis()
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
          //          df1.odbSimilarityWithKNNSearch(searchMetric, k, queryM, df1("metricDouble")).show()
          df1.odbSimilarityWithKNNSearch(searchMetric, k, queryM, df1("metricDouble"))
            //              .map(
            //                attr => {
            //                  val id = attr.getAs[Long](0)
            //                  val metricDouble = attr.getAs[mutable.WrappedArray[mutable.WrappedArray[Double]]](1).map(
            //                    x => if (x == null) null else x.toArray).toArray
            //                  val metricString = attr.getAs[mutable.WrappedArray[String]](2).map(
            //                    x => if (x == null) null else x).toArray
            //                  val metricRecord = MetricRecord(id, metricDouble, metricString, metricM, metricMaxDis)
            //                  (id, metricRecord.toString)
            //                }
            //              ).toDF("ID", "MetricRecord")
            .show(20, false)
          knnQueryTimeArray(m * queryKVal.length + i) = System.currentTimeMillis() - startTime
        }
      }
    }
    val rangeQueryAvg = rangeQueryTimeArray.sum / rangeQueryTimeArray.length
    val knnQueryAvg = knnQueryTimeArray.sum / knnQueryTimeArray.length

    (0,
      rangeQueryAvg,
      knnQueryAvg)

  }
}
