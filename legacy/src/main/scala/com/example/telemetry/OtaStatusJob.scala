package com.example.telemetry

import org.apache.spark.sql.functions.{col, max, struct, to_json}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Latest OTA status per VIN.
  *
  * Reads `ota_status` events, drops duplicates by `event_id` and keeps, per
  * VIN, the event with the greatest (`ts_ms`, `event_id`). Late events with an
  * older timestamp never overwrite a newer status. Output is written in update
  * mode to a topic keyed by VIN, meant to be log-compacted.
  *
  * Reads: event_id, vin, event_type, ts_ms, campaign_id, software_version,
  * status. Writes: vin, campaign_id, software_version, status, status_ts_ms,
  * source_event_id. Location is not read.
  */
object OtaStatusJob {

  def transform(events: DataFrame, watermark: String): DataFrame =
    VehicleEvents
      .dropDuplicateEvents(
        events.where(
          col("event_type") === "ota_status" &&
            col("campaign_id").isNotNull &&
            col("software_version").isNotNull &&
            col("status").isNotNull
        ),
        watermark
      )
      .groupBy(col("vin"))
      .agg(
        max(
          struct(
            col("ts_ms"),
            col("event_id"),
            col("campaign_id"),
            col("software_version"),
            col("status")
          )
        ).as("latest")
      )
      .select(
        col("vin"),
        col("latest.campaign_id").as("campaign_id"),
        col("latest.software_version").as("software_version"),
        col("latest.status").as("status"),
        col("latest.ts_ms").as("status_ts_ms"),
        col("latest.event_id").as("source_event_id")
      )

  def main(args: Array[String]): Unit = {
    val cfg = JobConfig.fromEnv(defaultOutputTopic = "ota-status-latest")
    val spark = SparkSession.builder().appName("OtaStatusJob").getOrCreate()

    val statuses = transform(VehicleEvents.parse(VehicleEvents.readKafka(spark, cfg)), cfg.watermark)

    val query = statuses
      .select(col("vin").as("key"), to_json(struct(statuses.columns.map(col): _*)).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrapServers)
      .option("topic", cfg.outputTopic)
      .option("checkpointLocation", cfg.checkpointLocation)
      .outputMode("update")
      .start()

    println(s"OtaStatusJob started, query id ${query.id}")
    query.awaitTermination()
  }
}
