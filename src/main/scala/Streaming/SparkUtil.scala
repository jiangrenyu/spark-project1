package Streaming

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Connection}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, Minutes, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by 姜仁雨 on 2017/11/4.
 */
class SparkUtil {
  /**
   * 获取DStream
   * @param ssc
   * @param zk
   * @param group
   * @param topic
   * @param numThreads
   * @return
   */
     def createKafkaMessage(ssc:StreamingContext,zk :String,group :String,topic :String,numThreads:Int): DStream[(String,Long)] ={
       val topicMap = topic.split(",").map((_, numThreads.toInt)).toMap
       val lines = KafkaUtils.createStream(ssc, zk, group, topicMap).map(_._2)
       val words = lines.flatMap(_.split(","))
       val wordCounts = words.map(x => (x, 1L))
       val result = wordCounts.reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
       result
     }

  /**
   * 获取hbase连接
   * @return
   */
     def getConn(): Connection ={
       val hbaseconf = HBaseConfiguration.create()
       //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
       hbaseconf.set("hbase.zookeeper.quorum","hadoop1,Hadoop162,Hadoop166")
       //设置zookeeper连接端口，默认2181
       hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
       val conn = ConnectionFactory.createConnection(hbaseconf)
       conn
     }
}
