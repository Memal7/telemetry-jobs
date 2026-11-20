package com.example.telemetry

import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

final case class Location(lat: Double, lon: Double)

final case class ChargingEvent(
    event_id: String,
    vin: String,
    event_type: String,
    ts_ms: Long,
    soc_pct: Option[Double],
    energy_kwh: Option[Double],
    location: Option[Location]
)

final case class ChargingSession(
    vin: String,
    start_event_id: String,
    stop_event_id: String,
    start_ts_ms: Long,
    stop_ts_ms: Long,
    duration_s: Long,
    start_soc_pct: Option[Double],
    end_soc_pct: Option[Double],
    energy_kwh: Option[Double],
    location: Option[Location]
)

/** Events not yet behind the watermark, plus the start of an open session. */
final case class SessionState(pending: Seq[ChargingEvent], open: Option[ChargingEvent])

/** One record per charging session, built from start and stop events.
  *
  * Per VIN, events are de-duplicated by `event_id`, buffered until the
  * watermark passes them and then processed in event order (`ts_ms`, then
  * `event_id`):
  *   - `charging_start` opens a session, replacing an unfinished one,
  *   - `charging_stop` closes the open session, or is ignored if none is open,
  *   - a session open longer than [[MaxOpenMs]] is discarded.
  *
  * Reads: event_id, vin, event_type, ts_ms, soc_pct, energy_kwh, location.
  * Writes: vin, start_event_id, stop_event_id, start_ts_ms, stop_ts_ms,
  * duration_s, start_soc_pct, end_soc_pct, energy_kwh, location (of the start).
  */
object ChargingSessionJob {

  val MaxOpenMs: Long = 24L * 60 * 60 * 1000

  private val Start = "charging_start"
  private val Stop = "charging_stop"

  def pair(
      vin: String,
      ordered: Seq[ChargingEvent],
      open: Option[ChargingEvent]
  ): (Vector[ChargingSession], Option[ChargingEvent]) =
    ordered.foldLeft((Vector.empty[ChargingSession], open)) {
      case ((out, _), e) if e.event_type == Start => (out, Some(e))
      case ((out, Some(start)), e) if e.event_type == Stop =>
        (out :+ session(vin, start, e), None)
      case (acc, _) => acc
    }

  private def session(vin: String, start: ChargingEvent, stop: ChargingEvent): ChargingSession =
    ChargingSession(
      vin = vin,
      start_event_id = start.event_id,
      stop_event_id = stop.event_id,
      start_ts_ms = start.ts_ms,
      stop_ts_ms = stop.ts_ms,
      duration_s = (stop.ts_ms - start.ts_ms) / 1000,
      start_soc_pct = start.soc_pct,
      end_soc_pct = stop.soc_pct,
      energy_kwh = stop.energy_kwh,
      location = start.location
    )

  /** `flushAll` processes every event at once; used for bounded replays. */
  def updateSessions(flushAll: Boolean)(
      vin: String,
      events: Iterator[ChargingEvent],
      state: GroupState[SessionState]
  ): Iterator[ChargingSession] = {
    val current = state.getOption.getOrElse(SessionState(Seq.empty, None))
    val sorted = (current.pending ++ events).sortBy(e => (e.ts_ms, e.event_id))
    val watermark = if (flushAll) Long.MaxValue else state.getCurrentWatermarkMs()
    val (ready, later) = sorted.partition(_.ts_ms <= watermark)

    val (sessions, stillOpen) = pair(vin, ready, current.open)
    val open = stillOpen.filter(s => flushAll || s.ts_ms + MaxOpenMs > watermark)

    if (later.isEmpty && open.isEmpty) state.remove()
    else {
      state.update(SessionState(later, open))
      if (!flushAll) {
        val next = later.headOption.map(_.ts_ms).getOrElse(open.get.ts_ms + MaxOpenMs)
        state.setTimeoutTimestamp(next)
      }
    }
    sessions.iterator
  }

  def transform(events: DataFrame, watermark: String, flushAll: Boolean = false): Dataset[ChargingSession] = {
    import events.sparkSession.implicits._
    VehicleEvents
      .dropDuplicateEvents(events.where(col("event_type").isin(Start, Stop)), watermark)
      .select("event_id", "vin", "event_type", "ts_ms", "soc_pct", "energy_kwh", "location")
      .as[ChargingEvent]
      .groupByKey(_.vin)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.EventTimeTimeout)(
        updateSessions(flushAll) _
      )
  }

  def main(args: Array[String]): Unit = {
    val cfg = JobConfig.fromEnv(defaultOutputTopic = "charging-sessions")
    val spark = SparkSession.builder().appName("ChargingSessionJob").getOrCreate()

    val sessions = transform(VehicleEvents.parse(VehicleEvents.readKafka(spark, cfg)), cfg.watermark)

    val query = sessions.toDF()
      .select(col("vin").as("key"), to_json(struct(col("*"))).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrapServers)
      .option("topic", cfg.outputTopic)
      .option("checkpointLocation", cfg.checkpointLocation)
      .outputMode("append")
      .start()

    println(s"ChargingSessionJob started, query id ${query.id}")
    query.awaitTermination()
  }
}
