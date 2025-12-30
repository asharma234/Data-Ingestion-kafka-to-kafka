package xxx.xxx.xxx.xxx.transform

import xxx.xxx.xxx.xxx.driver.PayloadDriver.logger
import xxx.xxx.xxx.xxx.utils.CONSTANTS._
import xxx.xxx.xxx.xxx.utils.Udf._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OktaSchema extends App {

  /**
   * @ process_batch => Method to convert string and return json message format
   */
  val YELLOW_BRIGHT = "\033[0;93m"
  val NORMAL = "\033[0m"
  val MAGENTA = "\033[1;35m"

  def processBatch(ss: SparkSession, kafkaSinkTopic: String, kafkaBootstrapConfig: String, payload: DataFrame, batchId: Long): Unit = {

    val startTimeMillis: Long = System.currentTimeMillis()

    logger.info(s"""Payload batch $batchId conversion from string to JSON Initiated """)
    import ss.implicits._
    val DataDf = payload.as[String]
      .map(_.replace("""\\\""", "")
        .replace("\\n", "")
        .replace("""\""", "")
        .replace(""""{""", """{"""")
        .replace(""""message":"""", """"message":""")
        .replace(""""}}"""", """}}"""))
      .withColumn(XXX_UUID, generateUUID())
      .withColumn(XXX_TIMESTAMP, generate_xxx_timestamp_udf())


    logger.info("Execution happens only when the Dataframe is having records")

    if (!DataDf.isEmpty) {
      val jsonDataLoad = DataDf
        .toJSON.map(_.replace("""\""", "")
        .replace("""{"value":"{"metadata":""","""{"metadata":""")
        .replace("""}}}}"""", """}}}""")
        .replace(""""aktaRequestPayload":{""""", """"aktaRequestPayload":{"""")
        .replace(""""aktaResponsePayload":{""""", """"aktaResponsePayload":{"""")
        .replace("\"[", "[")
        .replace("]\"", "]")
        .replace("]]", "]\"]")
        .replace("{\"\"", "{\"")
        .replace("}\"", "}")
        .replace("\"{", "{"))

      writeToKafka(jsonDataLoad, kafkaBootstrapConfig, kafkaSinkTopic)
    }

    else
    {
      logger.info(s"$YELLOW_BRIGHT Payload batch $batchId is Empty $NORMAL")
    }
    val endTimeMillis = System.currentTimeMillis()

    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

    logger.info("Minutes " + (durationSeconds / 60) + " Seconds " + (durationSeconds % 60))
  }
}


