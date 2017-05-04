package com.tomekl007.sparkstreaming

import java.time.{ZoneOffset, ZonedDateTime}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.scalatest.Matchers._

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

  test("should process all PageViews and sort them") {
    //given
    val pageView1 =
      PageView(1, "userId1", "www.proper-url.com", ZonedDateTime.now(ZoneOffset.UTC))
    val pageView2 =
      PageView(2, "userId1", "www.proper-url.com/login", ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(1))

    val input = spark.makeRDD(Seq(pageView2, pageView1))

    //when
    val sorted = FilterBotsJob.sort(input).collect().toList
    sorted should contain theSameElementsAs List(
      pageView1, pageView2
    )

    //and
    val pageView3 =
      PageView(3, "userId1", "www.proper-url.com/buy", ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(6))
    val pageView4 =
      PageView(4, "userId1", "www.proper-url.com/item/1", ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(5))


    val input2 = spark.makeRDD(Seq(pageView3, pageView4))

    //when
    val sorted2 = FilterBotsJob.sort(input2).collect().toList

    //then
    sorted2 should contain theSameElementsAs List(
      pageView3, pageView4
    )
  }

  test("should drop all not-in-order pageViews") {
    //given
    val pageView3 =
      PageView(3, "userId1", "www.proper-url.com/buy", ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(6))
    val pageView4 =
      PageView(4, "userId1", "www.proper-url.com/item/1", ZonedDateTime.now(ZoneOffset.UTC))

    //when
    val sorted = FilterBotsJob.dropOutOfOrderEvents(spark.makeRDD(List(pageView3))).collect().toList

    //then
    sorted should contain theSameElementsAs List(
      pageView3
    )

    //and
    val sorted2 = FilterBotsJob.dropOutOfOrderEvents(spark.makeRDD(List(pageView4))).collect().toList

    //then
    sorted2 should contain theSameElementsAs List()

  }

}