package Streaming


import org.apache.hadoop.hbase.TableName
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes

import scala.util.Try
/**
 * Created by 姜仁雨 on 2017/11/4.
 */
object KafkaTOHbaseDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Networkcount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("E:\\系统文件\\开发测试\\spark\\ck1")
    val topics = "topic01";
    val brokers = "hadoop1"
    val result = new SparkUtil().createKafkaMessage(ssc,brokers,"1",topics,2)
    result.print()+"---------------------------kafka输出数据"
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {//循环分区
        //try {
          val connection = new SparkUtil().getConn() //获取HBase连接,分区创建一个连接，分区不跨节点，不需要序列化
          partitionRecords.foreach(s => {
            //val data = JSON.parseObject(s._2)//将数据转化成JSON格式
            val data = s._2
            val tableName = TableName.valueOf("account2")
            val table = connection.getTable(tableName) //获取表连接

            val put = new Put(Bytes.toBytes(s._1))
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("num"), Bytes.toBytes(s._2))
            if(put!=null){
              print("成功写入hbase。。。。。。。。。。。。。。")
            }
            Try(table.put(put)).getOrElse(table.close()) //将数据写入HBase，若出错关闭table
            table.close() //分区数据写入HBase后关闭连接
          })
        //}
//        } catch {
//          case e: Exception => logger.error("写入HBase失败，{}", e.getMessage)
//        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
