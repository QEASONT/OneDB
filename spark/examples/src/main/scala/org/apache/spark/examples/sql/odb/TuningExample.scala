//package org.apache.spark.examples.sql.odb
//
//import java.util.logging.Logger
//import io.grpc.{Server, ServerBuilder}
//
//import scala.concurrent.{ExecutionContext, Future}
//import org.apache.spark.sql.tuning.GreeterGrpc.Greeter
//import org.apache.spark.sql.tuning.{GreeterGrpc, HelloReply, HelloRequest, PerformanceReply, PerformanceRequest}
//import org.apache.spark.examples.sql.odb.TuningMain
//
//
//object TuningExample {
//  val logger = Logger.getLogger(classOf[TuningExample].getName)
//
//  def main(args: Array[String]): Unit = {
//    val server = new TuningExample(ExecutionContext.global)
//    server.start()
//    server.blockUntilShutdown()
//  }
//
//  private val port = 50051
//}
//
//class TuningExample(executionContext: ExecutionContext) {
//  self =>
//  private[this] var server: Server = null
//  private[this] val logger = Logger.getLogger(classOf[TuningExample].getName)
//
//  def start(): Unit = {
//    server = ServerBuilder.forPort(TuningExample.port)
//      .addService(GreeterGrpc.bindService(new GreeterImpl, executionContext))
//      .build
//      .start
//    TuningExample.logger.info("Server started, listening on " + TuningExample.port)
//    sys.addShutdownHook {
//      System.err.println("*** shutting down gRPC server since JVM is shutting down")
//      self.stop()
//      System.err.println("*** server shut down")
//    }
//  }
//
//  def stop(): Unit = {
//    if (server != null) {
//      server.shutdown()
//    }
//  }
//
//  def blockUntilShutdown(): Unit = {
//    if (server != null) {
//      server.awaitTermination()
//    }
//  }
//
//  private class GreeterImpl extends Greeter {
//
//    override def getPerformanceMetrics(req: PerformanceRequest): Future[PerformanceReply] = Future.successful {
//      // Initialize Spark session
//      val reply = TuningMain.runSpark(req)
//      reply
//    }
//
//    override def sayHello(request: HelloRequest): Future[HelloReply] = {
//      val reply = HelloReply(message = 1)
//      Future.successful(reply)
//    }
//  }
//
//}
//
