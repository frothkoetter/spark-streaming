package com.tomekl007.sparkstreaming

import com.tomekl007.sparkstreaming.config._
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
    stream.filter(record => {
      !record.url.contains("bot")
    }).cache()
  }
}

object FilterBotsJob {

  def main(args: Array[String]): Unit = {
    val stream: DStream[PageView] = DStreamProvider.providePageViews()
    val sink: DStreamSink[PageView] = new DStreamSink()

    val job = new FilterBotsJob(stream, sink)

    job.start()
  }
}

