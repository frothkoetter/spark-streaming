package com.tomekl007.sparkstreaming.config

import org.apache.avro.generic.GenericData
import org.apache.spark.{SparkConf, SparkContext}

trait SparkApplication {

  def sparkAppName: String

  def withSparkContext(conf: (SparkConf => SparkConf))(f: (SparkContext) => Unit): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(sparkAppName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sparkConf.registerKryoClasses(Array(classOf[GenericData]))

    val sc = new SparkContext(conf(sparkConf))

    f(sc)
  }

}