package com.tomekl007.sparkstreaming

import org.apache.spark.streaming.dstream.DStream

object DStreamProvider {
  def providePageViews(): DStream[PageView] = {
    null //provide pageView's stream
  }

}
