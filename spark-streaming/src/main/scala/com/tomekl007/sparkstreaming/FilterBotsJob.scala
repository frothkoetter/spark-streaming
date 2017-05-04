package com.tomekl007.sparkstreaming

import java.time.ZonedDateTime

import com.tomekl007.sparkstreaming.config._
import com.tomekl007.sparkstreaming.ordering.StreamOrderVerification
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration._


class FilterBotsJob(source: DStream[PageView],
                    pageViewsSink: DStreamSink[PageView])
  extends SparkStreamingApplication {

  override def sparkAppName: String = "filter_bots_job"

  override def sparkStreamingConfig: SparkStreamingApplicationConfig =
    SparkStreamingApplicationConfig(Duration(2, SECONDS), "file://\"${java.io.tmpdir}")

  def start(): Unit = {
    withSparkStreamingContext { ssc =>
      processStream(ssc, source, pageViewsSink)
    }
  }

  def processStream(ssc: StreamingContext, stream: DStream[PageView],
                    sink: DStreamSink[PageView]): Unit = {
    val streamWithTaggedRecords = processPageViews(stream)
    sink.write(ssc, streamWithTaggedRecords)
  }

  def processPageViews(stream: DStream[PageView]): DStream[PageView] = {

    stream.transform(FilterBotsJob.sort(_))
      .filter(record => {
        !record.url.contains("bot")
      }).cache()
  }
}

object FilterBotsJob {

  val streamOrderVerification = new StreamOrderVerification()

  def main(args: Array[String]): Unit = {
    val stream: DStream[PageView] = DStreamProvider.providePageViews()
    val sink: DStreamSink[PageView] = new DStreamSink()

    val job = new FilterBotsJob(stream, sink)

    job.start()
  }

  def sort(rdd: RDD[PageView]): RDD[PageView] = {
    implicit val localDateOrdering: Ordering[ZonedDateTime] = Ordering.by(_.toInstant.toEpochMilli)
    rdd.sortBy(v => {
      v.eventTime
    }, ascending = true)
  }

  //if we want to have a strict order, we want to drop all our of order events
  def dropOutOfOrderEvents(rdd: RDD[PageView]): RDD[PageView] =
    rdd.filter(streamOrderVerification.isInOrder)

}

