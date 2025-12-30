package xxx.xxx.xxx.xxx.driver
import xxx.xxx.xxx.xxx.kafka.KafkaStream.defining_kafka_stream_payload
import xxx.xxx.xxx.xxx.transform.projSchema.processBatch
import xxx.xxx.xxx.xxx.utils.EnviromentConfig.params
import xxx.xxx.xxx.xxx.utils.EnviromentConfig.EnvironmentMetadata._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import java.time.{LocalDateTime, ZoneId}

import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

/**
  * @author : Arpan Sharma
  *         project : proj-src-to-sink
  *         package : xxx.xxx.xxx.xxx
  *         Date :    00/02/0000
  *
  */
object PayloadDriver extends App {

  val logger = org.apache.log4j.LogManager.getLogger(getClass)

  logger.info("executing kafka payload")


  val YELLOW_BRIGHT = "\033[0;93m"
  val NORMAL = "\033[0m"
  val MAGENTA = "\033[1;35m"

  logger.info("properties for application deployment")

  logger.info(
    s"""$MAGENTA########    Environment Settings      ###########$YELLOW_BRIGHT
    kafka_bootstrap_consumer: $kafkaBootstrapConfig
    kafka_raw_topic(s): $kafkaRawTopic
    kafka_sink_topic(s): $kafkaSinkTopic
    kafka_bootstrap_producer: $kafkaBootstrapConfig
    offsetValue: $offsetValue
    checkpoint_path:$checkpoint_path
      $MAGENTA-----------------------------------------------------------$NORMAL""")

  val sparkConf = new SparkConf()
  params.get.foreach { keyValue => sparkConf.set(keyValue._1, keyValue._2) }


  val ss: SparkSession = SparkSession
    .builder()
    .master("yarn")
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()


  val kafka_payload = defining_kafka_stream_payload(ss, kafkaBootstrapConfig, kafkaRawTopic, offsetValue)

  withRetry(write_stream_payload(ss, kafka_payload, kafkaSinkTopic, kafkaBootstrapConfig), max_retries.toInt, retry_delay_in_secs.toInt.seconds)

  def withRetry[T](operation: => T, maxRetries: Int, delay: FiniteDuration): T = {
    var result: Option[T] = None
    var retries = 0
    while (result.isEmpty && retries < maxRetries) {
      Try {
        result = Some(operation)
      } match {
        case Success(_) =>
        case Failure(ex) =>
          retries += 1
          if (retries < maxRetries) {
            println(s"Batch Time when failure occur -->  ${LocalDateTime.now(ZoneId.systemDefault)} Retry attempt $retries after ${delay.toSeconds} seconds due to: ${ex.getMessage}")
            Thread.sleep(delay.toMillis)
          } else {
            throw ex
          }
      }
    }
    result.getOrElse(throw new RuntimeException("Retry operation failed after maximum retries."))
  }

  def write_stream_payload(ss: SparkSession, incomingDf: DataFrame, kafkaSinkTopic: String, kafkaBootstrapConfig: String): Unit = {
    incomingDf
      .writeStream
      .option("checkpointLocation", checkpoint_path)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        processBatch(ss, kafkaSinkTopic, kafkaBootstrapConfig, batchDF, batchId)
      }
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .start()
      .awaitTermination()
  }

}
