package com.tomekl007.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, SparkStreamingSuite, Time}

import scala.collection.mutable

class WordCountStreamingTest extends SparkStreamingSuite {
  override def appName = "word-count-test"

  test("should calculate word count of streaming job") {
    val conf = new SparkConf().setMaster(s"local[2]").setAppName("word-count-app")

    val expectedOutput: Array[(String, Int)] = Array(
      "a" -> 3,
      "b" -> 2,
      "c" -> 4
    )

    val lines = List("a a b", "a b c", "c c c")

    val sentences = mutable.Queue[RDD[String]]()

    val streamingResults = mutable.ListBuffer.empty[Array[(String, Int)]]
    val wordCounts = ssc.queueStream(sentences).flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => streamingResults += rdd.collect)

    ssc.start()
    sentences += ssc.sparkContext.makeRDD(lines)
    assertInputMatchExpected(streamingResults, expectedOutput)
  }


  test("should window stream calculating word count per window") {
    val conf = new SparkConf().setMaster(s"local[2]").setAppName("word-count-app")

    val expectedOutput: Array[(String, Int)] = Array(
      "a" -> 2,
      "b" -> 1
    )

    val lines = List("a a b", "a b c", "c c c")

    val sentences = mutable.Queue[RDD[String]]()

    val streamingResults = mutable.ListBuffer.empty[Array[(String, Int)]]
    val wordCounts = ssc.queueStream(sentences).flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wordCounts
      .window(Seconds(1))
      .foreachRDD((rdd: RDD[(String, Int)], time: Time) => streamingResults += rdd.collect)

    ssc.start()
    sentences += ssc.sparkContext.makeRDD(List(lines.head))
    Thread.sleep(2000)
    sentences += ssc.sparkContext.makeRDD(lines.slice(1, 2))
    assertInputMatchExpected(streamingResults, expectedOutput)

  }
}
