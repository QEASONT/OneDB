package org.apache.spark.examples.sql.odb

import org.apache.spark.{SecurityManager, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.rpc._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SecurityManager, SparkConf}
import py4j.GatewayServer

import java.net.InetAddress
import java.util

object SparkRPCServerExample {

  def main(args: Array[String]): Unit = {
    val host = InetAddress.getLocalHost()
    try {
      val server = new GatewayServer(new App, 25333, 0, host, null, 0, 0, null)
      println("Starting Py4J GatewayServer on " + host + "...")
      server.start()
    }
  }
}


class App {
  def sayHello(string: String): String = {
    "Hello, " + string
  }

  def runSpark(sampleSize: Int,
               globalIndexedPivotCount: Int,
               rtreeGlobalMaxEntriesPerNode: Int,
               rtreeLocalMaxEntriesPerNode: Int,
               rtreeGlobalNumPartitions: Int,
               rtreeLocalNumPartitions: Int
              ): util.ArrayList[Long] = {
    TuningMain.runSpark(sampleSize, globalIndexedPivotCount, rtreeGlobalMaxEntriesPerNode, rtreeLocalMaxEntriesPerNode, rtreeGlobalNumPartitions, rtreeLocalNumPartitions)
  }
}