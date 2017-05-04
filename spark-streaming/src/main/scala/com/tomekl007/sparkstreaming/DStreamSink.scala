package com.tomekl007.sparkstreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


class DStreamSink[T] {
  def write(ssc: StreamingContext, source: DStream[PageViewWithViewCounter]) = {
    //send data to any sink
  }

}
