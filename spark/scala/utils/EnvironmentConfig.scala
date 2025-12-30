package xxx.xxx.xxx.xxx.utils

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

/**
  * Configurations for STREAMING_MODULENAME_SRC_2_SINK Streaming Job
  *
  * @param
  */
object EnviromentConfig  {
  lazy val config = ConfigFactory.load()
  lazy val envConfig = config.as[Option[Config]]("DataExport").getOrElse(ConfigFactory.empty())
  private lazy val spark = config.getConfig("spark")
  lazy val params = spark.as[Option[Map[String, String]]]("sparkConf")


  object EnvironmentMetadata {

    val kafkaBootstrapConfig: String = envConfig.as[Option[String]]("kafkaBootstrapConfig").getOrElse("")
    val kafkaRawTopic: String = envConfig.as[Option[String]]("kafkaRawTopic").getOrElse("")
    val kafkaSinkTopic: String = envConfig.as[Option[String]]("kafkaSinkTopic").getOrElse("")
    val checkpoint_path: String = envConfig.as[Option[String]]("checkpoint_path").getOrElse("")
    val offsetValue: String = envConfig.as[Option[String]]("offsetValue").getOrElse("")
    val max_retries: String = envConfig.as[Option[String]]("max_retries").getOrElse("")
    val retry_delay_in_secs: String = envConfig.as[Option[String]]("retry_delay_in_secs").getOrElse("")

  }
}
