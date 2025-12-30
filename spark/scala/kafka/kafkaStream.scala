package xxx.xxx.xxx.xxx.kafka

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaStream {

  def defining_kafka_stream_payload(ss: SparkSession, kafkaBootstrapConfig: String, kafkaRawTopic: String, offsetValue:String): DataFrame = {

    val kafka_stream = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapConfig)
      .option("subscribe", kafkaRawTopic)
      .option("startingOffsets", offsetValue)
      .option("maxOffsetsPerTrigger", 50000)
      .option("minOffsetsPerTrigger", 1)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.sasl.mechanism", "GSSAPI")
      .load()

    kafka_stream
      .select(col("value").cast("string"))

  }
}
