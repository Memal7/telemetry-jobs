package com.example.telemetry

import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Shared input contract: vehicle events as JSON values on a Kafka topic. */
object VehicleEvents {

  val schema: StructType = new StructType()
    .add("event_id", StringType)
    .add("vin", StringType)
    .add("event_type", StringType)
    .add("ts_ms", LongType)
    .add("campaign_id", StringType)
    .add("software_version", StringType)
    .add("status", StringType)
    .add("soc_pct", DoubleType)
    .add("energy_kwh", DoubleType)
    .add("location", new StructType().add("lat", DoubleType).add("lon", DoubleType))

  /** Parses a `value` column of JSON strings. Malformed values parse to nulls
    * and are dropped together with events missing an envelope field.
    */
  def parse(raw: DataFrame): DataFrame =
    raw
      .select(from_json(col("value").cast("string"), schema).as("e"))
      .select("e.*")
      .where(
        col("event_id").isNotNull && col("event_id") =!= "" &&
          col("vin").isNotNull && col("vin") =!= "" &&
          col("event_type").isNotNull && col("ts_ms").isNotNull
      )
      .withColumn("event_time", expr("timestamp_millis(ts_ms)"))

  /** Drops repeated `event_id`s. Streams keep ids for the watermark delay;
    * bounded replays (batch) de-duplicate over the whole input.
    */
  def dropDuplicateEvents(events: DataFrame, watermark: String): DataFrame =
    if (events.isStreaming)
      events.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark("event_id")
    else events.dropDuplicates("event_id")

  def readKafka(spark: SparkSession, cfg: JobConfig): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrapServers)
      .option("subscribe", cfg.inputTopic)
      .option("startingOffsets", "earliest")
      .load()
}

/** Job settings from environment variables. Never log record contents:
  * VIN and location are personal data.
  */
final case class JobConfig(
    bootstrapServers: String,
    inputTopic: String,
    outputTopic: String,
    checkpointLocation: String,
    watermark: String
)

object JobConfig {
  def fromEnv(defaultOutputTopic: String): JobConfig = {
    def env(name: String, default: String): String = sys.env.getOrElse(name, default)
    JobConfig(
      bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      inputTopic = env("INPUT_TOPIC", "vehicle-events"),
      outputTopic = env("OUTPUT_TOPIC", defaultOutputTopic),
      checkpointLocation = env("CHECKPOINT_LOCATION", s"/tmp/checkpoints/$defaultOutputTopic"),
      watermark = env("WATERMARK", "10 minutes")
    )
  }
}
