/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import java.io._
import java.util.{Locale, ArrayList => JArrayList}
import scala.collection.JavaConverters._
import jline.console.ConsoleReader
import jline.console.history.FileHistory
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.{CliDriver, CliSessionState, OptionsProcessor}
import org.apache.hadoop.hive.common.{HiveInterruptCallback, HiveInterruptUtils}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.log4j.{Level, Logger}
import org.apache.thrift.transport.TSocket
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.execution.odb.util.MetricRecord
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.util.ShutdownHookManager

/**
 * This code doesn't support remote connections in Hive 1.2+, as the underlying CliDriver
 * has dropped its support.
 */
private[hive] object SparkSQLCLIDriver extends Logging {
  private val prompt = "spark-sql"
  private val continuedPrompt = "".padTo(prompt.length, ' ')
  private var transport: TSocket = _

  installSignalHandler()

  /**
   * Install an interrupt callback to cancel all Spark jobs. In Hive's CliDriver#processLine(),
   * a signal handler will invoke this registered callback if a Ctrl+C signal is detected while
   * a command is being processed by the current thread.
   */
  def installSignalHandler() {
    HiveInterruptUtils.add(new HiveInterruptCallback {
      override def interrupt() {
        // Handle remote execution mode
        if (SparkSQLEnv.sparkContext != null) {
          SparkSQLEnv.sparkContext.cancelAllJobs()
        } else {
          if (transport != null) {
            // Force closing of TCP connection upon session termination
            transport.getSocket.close()
          }
        }
      }
    })
  }

  def main(args: Array[String]) {
    val oproc = new OptionsProcessor()
    if (!oproc.process_stage1(args)) {
      System.exit(1)
    }

    val cliConf = new HiveConf(classOf[SessionState])
    // Override the location of the metastore since this is only used for local execution.
    HiveUtils.newTemporaryConfiguration(useInMemoryDerby = false).foreach {
      case (key, value) => cliConf.set(key, value)
    }
    val sessionState = new CliSessionState(cliConf)

    sessionState.in = System.in
    try {
      sessionState.out = new PrintStream(System.out, true, "UTF-8")
      sessionState.info = new PrintStream(System.err, true, "UTF-8")
      sessionState.err = new PrintStream(System.err, true, "UTF-8")
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (!oproc.process_stage2(sessionState)) {
      System.exit(2)
    }

    // Set all properties specified via command line.
    val conf: HiveConf = sessionState.getConf
    sessionState.cmdProperties.entrySet().asScala.foreach { item =>
      val key = item.getKey.toString
      val value = item.getValue.toString
      // We do not propagate metastore options to the execution copy of hive.
      if (key != "javax.jdo.option.ConnectionURL") {
        conf.set(key, value)
        sessionState.getOverriddenConfigurations.put(key, value)
      }
    }

    SessionState.start(sessionState)

    // Clean up after we exit
    ShutdownHookManager.addShutdownHook { () => SparkSQLEnv.stop() }

    val remoteMode = isRemoteMode(sessionState)
    // "-h" option has been passed, so connect to Hive thrift server.
    if (!remoteMode) {
      // Hadoop-20 and above - we need to augment classpath using hiveconf
      // components.
      // See also: code in ExecDriver.java
      var loader = conf.getClassLoader
      val auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS)
      if (StringUtils.isNotBlank(auxJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","))
      }
      conf.setClassLoader(loader)
      Thread.currentThread().setContextClassLoader(loader)
    } else {
      // Hive 1.2 + not supported in CLI
      throw new RuntimeException("Remote operations not supported")
    }

    val cli = new SparkSQLCLIDriver
    cli.setHiveVariables(oproc.getHiveVariables)

    // TODO work around for set the log output to console, because the HiveContext
    // will set the output into an invalid buffer.
    sessionState.in = System.in
    try {
      sessionState.out = new PrintStream(System.out, true, "UTF-8")
      sessionState.info = new PrintStream(System.err, true, "UTF-8")
      sessionState.err = new PrintStream(System.err, true, "UTF-8")
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (sessionState.database != null) {
      SparkSQLEnv.sqlContext.sessionState.catalog.setCurrentDatabase(
        s"${sessionState.database}")
    }

    // Execute -i init files (always in silent mode)
    cli.processInitFiles(sessionState)

    // Respect the configurations set by --hiveconf from the command line
    // (based on Hive's CliDriver).
    val it = sessionState.getOverriddenConfigurations.entrySet().iterator()
    while (it.hasNext) {
      val kv = it.next()
      SparkSQLEnv.sqlContext.setConf(kv.getKey, kv.getValue)
    }

    //    val spark = SparkSQLEnv.sqlContext.sparkSession
    //
    //    // For implicit conversions like converting RDDs to DataFrames
    //    import spark.implicits._
    //
    //
    //    val sc = spark.sparkContext
    //    val fileData = "examples/src/main/resources/dfood.txt"
    //    val fileContents = sc.textFile(fileData)
    //
    //    val headerInfo = fileContents.take(3)
    //    // read num of rows and columns
    //    val n = headerInfo(0).split(" ")(0).toInt
    //    val m = headerInfo(0).split(" ")(1).toInt
    //
    //    // read metricm and metricMaxDis
    //    val metricM = headerInfo(1).split(" ").map(x => x.toInt)
    //    val metricMaxDis = headerInfo(2).split(" |\t").map(x => x.toDouble)
    //    val metricData = fileContents.zipWithIndex().filter(_._2 >= 3).map(MetricRecord.getMetric(_, metricM, metricMaxDis))
    //    //    val metricMeanDis = MetricRecord.calculateMetricMeanDis(metricData, metricM, metricMaxDis)
    //
    //    val queryData = "examples/src/main/resources/dfood_q.txt"
    //    val queryContents = sc.textFile(queryData)
    //    val queryHeaderInfo = queryContents.take(4)
    //    val queryM = queryHeaderInfo(0).split(" ").map(x => x.toInt)
    //    val queryKVal = queryHeaderInfo(1).split(" |\t").zipWithIndex.filter(_._2 > 0).map(x => x._1.toInt)
    //    val queryRad = queryHeaderInfo(3).split(" |\t").map(x => x.toDouble)
    //    val queryPos = queryContents.zipWithIndex().filter(_._2 >= 4).map(x => x._1.toInt).collect()
    //    //
    //    //    val df1 = metricData.toDF()
    //    val df1 = spark.sql("select * from qtspark.multimetric")
    ////    df1.count()
    //    df1.createOrReplaceTempView("metric1")
    //    df1.createODBIndex(df1("metricDouble"), df1("metricString"), df1("metricM"), df1("metricMaxDis"), "metric_index1")
    //
    //    for (m <- queryPos.indices) {
    //      for (i <- queryRad.indices) {
    //        val radius = queryRad(i)
    //        val filterQueryP = metricData.filter(t => t.id == queryPos(m))
    //        if (filterQueryP.count() != 0) {
    //          val queryP = filterQueryP.take(1).head
    //          val pintsArray: Array[Point[Any]] = metricM.zipWithIndex.map(x =>
    //            x._1 match {
    //              case 0 | 1 | 2 | 3 | 4 | 5 =>
    //                Point[Any](queryP.metricDouble(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
    //              case 6 =>
    //                Point[Any](queryP.metricString(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
    //            })
    //          val searchMetric = MetricData(pintsArray)
    //
    //          df1.odbSimilarityWithRangeSearch(searchMetric, radius, queryM, df1("metricDouble")).show()
    //
    //          val queryMetricStr = "ODBPOINT(" + searchMetric.points.map(point =>
    //            s"${point.toString}").mkString("|") + s"),METRICM(${metricM.mkString(",")}),QUERYM(${queryM.mkString(",")})" + "," + radius
    //          //          val queryMetricStr = "ODBPOINT(1.0;580.53;7.29;0.96;11.34;1.651;'Plant-based,foods,and,beverages';-0.203939,0.047418,0.114095,0.35395,-0.128798,0.145616,0.070353,-0.11815,-0.090278,0.046224,0.050086,-0.107897,-0.013373,0.11518,-0.199978,0.06288,-0.141927,0.023244,0.073079,-0.093886,0.123237,-0.070354,0.093628,0.006863,-0.101345,-0.012614,-0.124607,0.105347,-0.020684,0.022766,-0.125317,-0.077355,0.103841,0.075819,-0.133708,0.024577,0.176948,-0.049615,0.064263,0.13208,-0.049072,-0.078016,0.074246,0.16314,-0.125895,-0.203966,-0.059272,0.139154,-0.009345,0.078342,-0.042996,-0.05447,-0.065782,-0.052694,0.00982,-0.126899,0.053786,-0.125644,-0.116014,-0.087796,-0.129645,0.092383,-0.120945,0.017361,-0.080644,-0.186768,-0.182726,0.004544,0.087264,-0.036567,0.18131,-0.052979,-0.030775,0.007867,-0.101128,-0.273546,-0.329536,-0.043972,0.011224,-0.148248,-0.019884,-0.11929,0.153225,0.004839,-0.002116,-0.1148,-0.13406,0.272407,-0.003682,-0.111274,-0.160251,-0.217773,-0.055935,-0.003645,-0.027832,-0.093309,0.011882,0.049622,0.158854,-0.113932;49.0,17.0,16.0,16.0,24.0,8.0,27.0,16.0,18.0,31.0,14.0,16.0,170.0,135.0,235.0,210.0,211.0,196.0,210.0,215.0,209.0,197.0,184.0,181.0,205.0,191.0,193.0,175.0,147.0,178.0,170.0,188.0,175.0,165.0,153.0,156.0,163.0,170.0,173.0,154.0,148.0,143.0,162.0,170.0,242.0,206.0,209.0,198.0,210.0,211.0,197.0,200.0,182.0,182.0,201.0,181.0,193.0,172.0,130.0,167.0,156.0,185.0,171.0,147.0,156.0,146.0,160.0,174.0,167.0,133.0,126.0,135.0,132.0,157.0,8.0,15.0,2.0,2.0,3.0,15.0,15.0,12.0,3.0,4.0,2.0,14.0,13.0,10.0,9.0,6.0,4.0,12.0,8.0,7.0,10.0,7.0,5.0,12.0,12.0,7.0,9.0,7.0,4.0,10.0,11.0,11.0,8.0,6.0,2.0,2.0,6.0,1.0,4.0,2.0,1.0,6.0,3.0,4.0,1.0,1.0,7.0,3.0,3.0,2.0,3.0,5.0,1.0,6.0,3.0,3.0,3.0,4.0,4.0,6.0,3.0,4.0,5.0,5.0,5.0,2.0,6.0,5.0,5.0,5.0,5.0,5.0,3.0,4.0,3.0,4.0,3.0,2.0,7.0,6.0,4.0,5.0,5.0,5.0,4.0,2.0,6.0,4.0,4.0,5.0,4.0,4.0,5.0,2.0,5.0,1.0,7.0,4.0,5.0,4.0,3.0,6.0,5.0,2.0,4.0,1.0,7.0,3.0,3.0,3.0,6.0,4.0,1.0,1.0,1.0),METRICM(0,0,0,0,0,0,6,5,4),QUERYM(1,0,0,0,0,0,0,1,1),0.294)"
    //          //          println(queryMetricStr)
    //          //                val aaa = s"SELECT * FROM metric1 WHERE metric1.metricDouble IN ODBRANGE" + s"(" + queryMetricStr + ")"
    //          spark.sql(s"use qtspark")
    //          spark.sql(s"SELECT * FROM metric1 WHERE metric1.metricDouble IN ODBRANGE" + s"(" + queryMetricStr + ")").show()
    //
    //        }
    //      }
    //      for (i <- queryKVal.indices) {
    //        val k = queryKVal(i)
    //        val filterQueryP = metricData.filter(t => t.id == queryPos(m))
    //        if (filterQueryP.count() != 0) {
    //          val queryP = filterQueryP.take(1).head
    //          val pintsArray: Array[Point[Any]] = metricM.zipWithIndex.map(x =>
    //            x._1 match {
    //              case 0 | 1 | 2 | 3 | 4 | 5 =>
    //                Point[Any](queryP.metricDouble(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
    //              case 6 =>
    //                Point[Any](queryP.metricString(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
    //            })
    //          val searchMetric = MetricData(pintsArray)
    //          val queryMetricStr = "ODBPOINT(" + searchMetric.points.map(point =>
    //            s"${point.toString}").mkString("|") + s"),METRICM(${metricM.mkString(",")}),QUERYM(${queryM.mkString(",")})" + "," + k
    //          println(queryMetricStr)
    //          //          val aaa = s"SELECT * FROM metric1 WHERE metric1.metric ODBKNN" + s"(" + queryMetricStr + s",$k)"
    //          spark.sql(s"SELECT * FROM metric1 WHERE metric1.metricDouble IN ODBKNN"
    //            + s"(" + queryMetricStr + ")").show()
    //        }
    //      }
    //    }


    if (sessionState.execString != null) {
      System.exit(cli.processLine(sessionState.execString))
    }

    try {
      if (sessionState.fileName != null) {
        System.exit(cli.processFile(sessionState.fileName))
      }
    } catch {
      case e: FileNotFoundException =>
        logError(s"Could not open input file for reading. (${e.getMessage})")
        System.exit(3)
    }

    val reader = new ConsoleReader()
    reader.setBellEnabled(false)
    reader.setExpandEvents(false)
    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)))
    CliDriver.getCommandCompleter.foreach((e) => reader.addCompleter(e))

    val historyDirectory = System.getProperty("user.home")

    try {
      if (new File(historyDirectory).exists()) {
        val historyFile = historyDirectory + File.separator + ".hivehistory"
        reader.setHistory(new FileHistory(new File(historyFile)))
      } else {
        logWarning("WARNING: Directory for Hive history file: " + historyDirectory +
          " does not exist.   History will not be available during this session.")
      }
    } catch {
      case e: Exception =>
        logWarning("WARNING: Encountered an error while trying to initialize Hive's " +
          "history file.  History will not be available during this session.")
        logWarning(e.getMessage)
    }

    // add shutdown hook to flush the history to history file
    ShutdownHookManager.addShutdownHook { () =>
      reader.getHistory match {
        case h: FileHistory =>
          try {
            h.flush()
          } catch {
            case e: IOException =>
              logWarning("WARNING: Failed to write command history file: " + e.getMessage)
          }
        case _ =>
      }
    }

    // TODO: missing
    /*
        val clientTransportTSocketField = classOf[CliSessionState].getDeclaredField("transport")
        clientTransportTSocketField.setAccessible(true)

        transport = clientTransportTSocketField.get(sessionState).asInstanceOf[TSocket]
    */
    transport = null

    var ret = 0
    var prefix = ""
    val currentDB = ReflectionUtils.invokeStatic(classOf[CliDriver], "getFormattedDb",
      classOf[HiveConf] -> conf, classOf[CliSessionState] -> sessionState)

    def promptWithCurrentDB: String = s"$prompt$currentDB"

    def continuedPromptWithDBSpaces: String = continuedPrompt + ReflectionUtils.invokeStatic(
      classOf[CliDriver], "spacesForString", classOf[String] -> currentDB)

    var currentPrompt = promptWithCurrentDB
    var line = reader.readLine(currentPrompt + "> ")


    while (line != null) {
      if (!line.startsWith("--")) {
        if (prefix.nonEmpty) {
          prefix += '\n'
        }

        if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
          line = prefix + line
          ret = cli.processLine(line, true)
          prefix = ""
          currentPrompt = promptWithCurrentDB
        } else {
          prefix = prefix + line
          currentPrompt = continuedPromptWithDBSpaces
        }
      }
      line = reader.readLine(currentPrompt + "> ")
    }

    sessionState.close()

    System.exit(ret)
  }


  def isRemoteMode(state: CliSessionState): Boolean = {
    //    sessionState.isRemoteMode
    state.isHiveServerQuery
  }

}

private[hive] class SparkSQLCLIDriver extends CliDriver with Logging {
  private val sessionState = SessionState.get().asInstanceOf[CliSessionState]

  private val LOG = LogFactory.getLog("CliDriver")

  private val console = new SessionState.LogHelper(LOG)

  if (sessionState.getIsSilent) {
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  private val isRemoteMode = {
    SparkSQLCLIDriver.isRemoteMode(sessionState)
  }

  private val conf: Configuration =
    if (sessionState != null) sessionState.getConf else new Configuration()

  // Force initializing SparkSQLEnv. This is put here but not object SparkSQLCliDriver
  // because the Hive unit tests do not go through the main() code path.
  if (!isRemoteMode) {
    SparkSQLEnv.init()
  } else {
    // Hive 1.2 + not supported in CLI
    throw new RuntimeException("Remote operations not supported")
  }

  override def setHiveVariables(hiveVariables: java.util.Map[String, String]): Unit = {
    hiveVariables.asScala.foreach(kv => SparkSQLEnv.sqlContext.conf.setConfString(kv._1, kv._2))
  }

  override def processCmd(cmd: String): Int = {
    val cmd_trimmed: String = cmd.trim()
    val cmd_lower = cmd_trimmed.toLowerCase(Locale.ROOT)
    val tokens: Array[String] = cmd_trimmed.split("\\s+")
    val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
    if (cmd_lower.equals("quit") ||
      cmd_lower.equals("exit")) {
      sessionState.close()
      System.exit(0)
    }
    if (tokens(0).toLowerCase(Locale.ROOT).equals("source") ||
      cmd_trimmed.startsWith("!") || isRemoteMode) {
      val start = System.currentTimeMillis()
      super.processCmd(cmd)
      val end = System.currentTimeMillis()
      val timeTaken: Double = (end - start) / 1000.0
      console.printInfo(s"Time taken: $timeTaken seconds")
      0
    } else {
      var ret = 0
      val hconf = conf.asInstanceOf[HiveConf]
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens, hconf)

      if (proc != null) {
        // scalastyle:off println
        if (proc.isInstanceOf[Driver] || proc.isInstanceOf[SetProcessor] ||
          proc.isInstanceOf[AddResourceProcessor] || proc.isInstanceOf[ListResourceProcessor] ||
          proc.isInstanceOf[ResetProcessor]) {
          val driver = new SparkSQLDriver

          driver.init()
          val out = sessionState.out
          val err = sessionState.err
          val start: Long = System.currentTimeMillis()
          if (sessionState.getIsVerbose) {
            out.println(cmd)
          }
          val rc = driver.run(cmd)
          //          SparkSQLEnv.sqlContext.sparkSession.sql(cmd).show()
          val end = System.currentTimeMillis()
          val timeTaken: Double = (end - start) / 1000.0

          ret = rc.getResponseCode
          if (ret != 0) {
            // For analysis exception, only the error is printed out to the console.
            rc.getException() match {
              case e: AnalysisException =>
                err.println(s"""Error in query: ${e.getMessage}""")
              case _ => err.println(rc.getErrorMessage())
            }
            driver.close()
            return ret
          }

          val res = new JArrayList[String]()

          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)) {
            // Print the column names.
            Option(driver.getSchema.getFieldSchemas).foreach { fields =>
              out.println(fields.asScala.map(_.getName).mkString("\t"))
            }
          }

          var counter = 0
          try {
            while (!out.checkError() && driver.getResults(res)) {
              res.asScala.foreach { l =>
                counter += 1
                out.println(l)
              }
              res.clear()
            }
          } catch {
            case e: IOException =>
              console.printError(
                s"""Failed with exception ${e.getClass.getName}: ${e.getMessage}
                   |${org.apache.hadoop.util.StringUtils.stringifyException(e)}
                           """.stripMargin)
              ret = 1
          }

          val cret = driver.close()
          if (ret == 0) {
            ret = cret
          }

          var responseMsg = s"Time taken: $timeTaken seconds"
          if (counter != 0) {
            responseMsg += s", Fetched $counter row(s)"
          }
          console.printInfo(responseMsg, null)
          // Destroy the driver to release all the locks.
          driver.destroy()
        } else {
          if (sessionState.getIsVerbose) {
            sessionState.out.println(tokens(0) + " " + cmd_1)
          }
          ret = proc.run(cmd_1).getResponseCode
        }
        // scalastyle:on println
      }
      ret
    }
  }
}

