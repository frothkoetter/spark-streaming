package com.tomekl007.sink

import com.tomekl007.WithUserId
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait Sink[T <: WithUserId] {
  def write(ssc: StreamingContext, result: DStream[T])

}
