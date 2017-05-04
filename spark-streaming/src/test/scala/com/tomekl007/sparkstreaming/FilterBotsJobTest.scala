package com.tomekl007.sparkstreaming

import java.time.{ZoneOffset, ZonedDateTime}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import scala.collection.mutable

class FilterBotsJobTest extends SparkStreamingSuite {

  private val underTest = new FilterBotsJob(null, null)

  override def appName: String = this.getClass.getSimpleName


  test("should filter all bots PageViews") {
    //given
    val pageView1 = PageView(1, "userId1", "www.proper-url.com", ZonedDateTime.now(ZoneOffset.UTC))
    val pageView2 = PageView(2, "userId1", "www.bot.com", ZonedDateTime.now(ZoneOffset.UTC))
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