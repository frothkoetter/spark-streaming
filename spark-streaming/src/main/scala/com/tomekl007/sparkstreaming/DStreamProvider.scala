package com.tomekl007.sparkstreaming

import kafka.serializer.DefaultDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object DStreamProvider {

  def providePageViews(ssc: StreamingContext): DStream[PageView] = {
    KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc,
      Map("auto.offset.reset" -> "largest"),
      Map("page_views" -> 1),
      StorageLevel.MEMORY_ONLY
    ).map(deserializeToPageView)
  }

  def deserializeToPageView(tuple: (Array[Byte], Array[Byte])): PageView = {
    //deserialization logic, kafka format dependent
    null
  }

}
