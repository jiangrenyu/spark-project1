package com.jry.spark1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, Minutes, Durations, StreamingContext}
import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
/**
 * Created by 姜仁雨 on 2017/11/2.
 *
 */
object KafkaToHdfs {
  def main(args: Array[String]) {
       val zk = "hadoop1"
       val group = "1";
       val topics = "topic5";
       val numberThreads = "2";

       val sparkConf = new SparkConf().setAppName("local").setMaster("local[2]")
       val jsc = new StreamingContext(sparkConf, Seconds(2))
       jsc.checkpoint("E:\\系统文件\\开发测试\\spark\\ck")

       val topicMap = topics.split(",").map((_, numberThreads.toInt)).toMap
       val lines = KafkaUtils.createStream(jsc, zk, group, topicMap).map(_._2)
       val words = lines.flatMap(_.split(","))
       val wordCounts = words.map(x => (x, 1L))

       val result = wordCounts.reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
       print("reduceByKeyAndWindow")
       result.print()
       print("flatMap............")
       words.print()
       print("map............")
       wordCounts.print()
       jsc.start()
       jsc.awaitTermination()

  }
}
