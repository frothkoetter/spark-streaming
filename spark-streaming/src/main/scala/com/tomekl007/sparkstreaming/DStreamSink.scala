package com.tomekl007.sparkstreaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


class DStreamSink[T] {
  def write(ssc: StreamingContext, result: DStream[PageViewWithViewCounter]) = {
    val kafkaProducer: Producer[Array[Byte], PageViewWithViewCounter] =
      new KafkaProducer[Array[Byte], PageViewWithViewCounter](new Properties())//supply real kafka config

    val producerVar = ssc.sparkContext.broadcast(kafkaProducer)
    val topicVar = ssc.sparkContext.broadcast("output_topic_name")

    result.foreachRDD { rdd =>
      rdd.foreach { record =>
        val topic = topicVar.value
        val producer = producerVar.value

        producer.send(
          new ProducerRecord(topic, record)
        )
      }
    }
  }

}
