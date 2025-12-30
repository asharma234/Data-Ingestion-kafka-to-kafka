package xxx.xxx.xxx.xxx.utils


import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset}

import java.util.UUID
import java.util.concurrent.TimeUnit

object Udf {
  val generateUUID: UserDefinedFunction = udf(() => UUID.randomUUID().toString)

  def generate_xxx_timestamp_udf: UserDefinedFunction = udf(generate_xxx_timestamp _)

  def generate_xxx_timestamp(): String = {
    String.valueOf(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
  }

  def writeToKafka(df: Dataset[String], kafkaBootstrapConfig: String, kafkaSinkTopic: String): Unit = {
    df.selectExpr("CAST(value AS STRING) as value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapConfig)
      .option("topic", kafkaSinkTopic)
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.security.protocol", "SASL_SSL")
      .option(s"kafka.${ProducerConfig.MAX_REQUEST_SIZE_CONFIG}", "10000000")
      .save()
  }


}
