package com.example.telemetry

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Runs ChargingSessionJob as a real streaming query (memory source and
  * sink, watermark delay 0) so that event-time state and timeouts are used.
  * All data is synthetic.
  */
class ChargingSessionJobSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("ChargingSessionJobSpec")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private val T0 = 1767225600000L
  private val Hour = 60L * 60 * 1000
  private val TickVin = "SYNTHVIN000000999"

  private def event(id: String, vin: String, eventType: String, ts: Long, extra: String = ""): String =
    s"""{"event_id":"$id","vin":"$vin","event_type":"$eventType","ts_ms":$ts$extra}"""

  private def start(id: String, vin: String, ts: Long, extra: String = ""): String =
    event(id, vin, "charging_start", ts, extra)

  private def stop(id: String, vin: String, ts: Long): String = event(id, vin, "charging_stop", ts)

  /** A stop without a start on another VIN: produces nothing, advances the watermark. */
  private def tick(ts: Long): Seq[String] = Seq(stop(s"tick-$ts", TickVin, ts))

  private final case class Run(sessions: Seq[ChargingSession], stateRows: Long)

  /** Feeds one micro-batch per element of `batches`, then two ticks far
    * ahead so every buffered event passes the watermark.
    */
  private def runStream(batches: Seq[String]*): Run = {
    import spark.implicits._
    implicit val ctx: SQLContext = spark.sqlContext
    val input = MemoryStream[String]
    val name = "sessions_" + java.util.UUID.randomUUID().toString.replace("-", "")
    val query = ChargingSessionJob
      .transform(VehicleEvents.parse(input.toDF()), "0 seconds")
      .writeStream
      .format("memory")
      .queryName(name)
      .outputMode("append")
      .start()
    try {
      (batches ++ Seq(tick(T0 + 100 * Hour), tick(T0 + 200 * Hour))).foreach { batch =>
        input.addData(batch)
        query.processAllAvailable()
      }
      val stateRows = query.lastProgress.stateOperators
        .filter(_.operatorName == "flatMapGroupsWithState")
        .map(_.numRowsTotal)
        .sum
      Run(spark.table(name).as[ChargingSession].collect().sortBy(_.start_event_id).toSeq, stateRows)
    } finally query.stop()
  }

  private def ids(run: Run): Seq[(String, String)] = run.sessions.map(s => (s.start_event_id, s.stop_event_id))

  test("sessions longer than 24 h are dropped when start and stop are in the same batch") {
    val run = runStream(
      Seq(
        start("a1", "SYNTHVIN000000001", T0),
        stop("a2", "SYNTHVIN000000001", T0 + 25 * Hour),
        stop("a3", "SYNTHVIN000000001", T0 + 26 * Hour),
        start("b1", "SYNTHVIN000000002", T0),
        stop("b2", "SYNTHVIN000000002", T0 + 2 * Hour),
        start("c1", "SYNTHVIN000000003", T0),
        stop("c2", "SYNTHVIN000000003", T0 + ChargingSessionJob.MaxOpenMs)
      )
    )
    assert(ids(run) == Seq("b1" -> "b2", "c1" -> "c2"))
  }

  test("sessions longer than 24 h are dropped when start and stop are in different watermarks") {
    val run = runStream(
      Seq(start("a1", "SYNTHVIN000000001", T0), start("b1", "SYNTHVIN000000002", T0)),
      tick(T0 + Hour),
      Seq(stop("a2", "SYNTHVIN000000001", T0 + 25 * Hour), stop("b2", "SYNTHVIN000000002", T0 + 3 * Hour))
    )
    assert(ids(run) == Seq("b1" -> "b2"))
  }

  test("an unmatched start is removed from state once the watermark passes 24 h") {
    val run = runStream(Seq(start("a1", "SYNTHVIN000000001", T0)), tick(T0 + 30 * Hour))
    assert(run.sessions.isEmpty)
    // Only the last tick is still buffered.
    assert(run.stateRows == 1)
  }

  test("a start with an incomplete location still yields a session") {
    val run = runStream(
      Seq(
        start("a1", "SYNTHVIN000000001", T0, extra = ""","location":{}"""),
        stop("a2", "SYNTHVIN000000001", T0 + Hour)
      )
    )
    assert(ids(run) == Seq("a1" -> "a2"))
    assert(run.sessions.head.location.contains(Location(None, None)))
  }

  test("pair: a stop more than MaxOpenMs after the start closes it without output") {
    def ev(id: String, eventType: String, ts: Long) =
      ChargingEvent(id, "SYNTHVIN000000001", eventType, ts, None, None, None)
    val (sessions, open) = ChargingSessionJob.pair(
      "SYNTHVIN000000001",
      Seq(
        ev("s", "charging_start", 0),
        ev("t", "charging_stop", ChargingSessionJob.MaxOpenMs + 1),
        ev("u", "charging_stop", ChargingSessionJob.MaxOpenMs + 2)
      ),
      None
    )
    assert(sessions.isEmpty)
    assert(open.isEmpty)
  }
}
