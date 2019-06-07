import org.apache.spark.SparkContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.KafkaUtils



val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "ip-10-0-1-187:6667",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "use_a_separate_group_id_for_each_stream",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)

val sc = SparkContext.getOrCreate
val ssc = new StreamingContext(sc, Seconds(10))

val topics = Array("gateway-europe-raw-sensors")
val preferredHosts = LocationStrategies.PreferConsistent

val stream = KafkaUtils.createDirectStream[String, String](
  ssc,
  preferredHosts,
  ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
)


stream.count.print()
stream.foreachRDD { rdd =>
  // Get the offset ranges in the RDD
  val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  for (o <- offsetRanges) {
    println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
}
}


stream.foreachRDD { rdd =>
 val spark = sc.builder.config(rdd.sparkContext.getConf).getOrCreate()
  import spark.implicits._

  // Convert RDD[String] to DataFrame
  val wordsDataFrame = rdd.toDF("stream")

  // Create a temporary view
  wordsDataFrame.createOrReplaceTempView("stream")

  // Do word count on DataFrame using SQL and print it
  val wordCountsDataFrame = 
    spark.sql("select *  from stream")
  wordCountsDataFrame.show()
}

ssc.start

// the above code is printing out topic details every 5 seconds
// until you stop it.

ssc.stop(stopSparkContext = false)
