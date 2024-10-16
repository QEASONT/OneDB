package org.apache.spark.examples.sql.odb

import org.apache.spark.{SecurityManager, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.rpc._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SecurityManager, SparkConf}


import py4j.GatewayServer

object SparkRPCServerExample {

  def main(args: Array[String]): Unit = {
    val server = new GatewayServer(new App, 25333, "localhost", 0, 0)
    server.start()
  }
}


class App {
  def addition(first: Int, second: Int): Int = first + second

  def runSpark(sampleSize: Int,
               globalIndexedPivotCount: Int,
               rtreeGlobalMaxEntriesPerNode: Int,
               rtreeLocalMaxEntriesPerNode: Int,
               rtreeGlobalNumPartitions: Int,
               rtreeLocalNumPartitions: Int
              ): (Int, Long, Long) = {
    TuningMain.runSpark(sampleSize, globalIndexedPivotCount, rtreeGlobalMaxEntriesPerNode, rtreeLocalMaxEntriesPerNode, rtreeGlobalNumPartitions, rtreeLocalNumPartitions)
  }
}