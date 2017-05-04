package com.tomekl007.sparkstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import scala.collection.mutable

class FilterBotsJobTest extends SparkStreamingSuite {

  private val underTest = new FilterBotsJob(null, null)

  override def appName: String = this.getClass.getSimpleName


  test("should classify all PV records properly in two batches") {
    //given
    val pageView1 = PageView("1", "www.proper-url.com")
    val pageView2 = PageView("2", "www.bot.com")
    val input = Seq(pageView1, pageView2)
    val expectedOutput: Array[PageView] = Array(
      pageView1
    )

    val pageViews = mutable.Queue[RDD[PageView]]()
    val streamingResults = mutable.ListBuffer.empty[Array[(PageView)]]
    val results = underTest.processPageViews(ssc.queueStream(pageViews))
    results.foreachRDD((rdd: RDD[(PageView)], time: Time) => streamingResults += rdd.collect)

    ssc.start()

    //when
    pageViews += spark.makeRDD(input)
    assertInputMatchExpected(streamingResults, expectedOutput)
  }

}