package Streaming

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, ConnectionFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.hadoop.hbase.{HTableDescriptor,HColumnDescriptor,HBaseConfiguration,TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql._

/**
 * Created by 姜仁雨 on 2017/11/3.
 */
object ToHbase1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Networkcount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val topics = "topic5";
    val brokers = "hadoop1"
    val topicMap = topics.split(",").map((_, "2".toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, brokers, "1", topicMap).map(_._2)
    var rank = 0; //用来记录当前数据序号
    val sqlcontext = new SQLContext(sc)
    import sqlcontext.implicits._

    val lines1 = lines.window(Seconds(10), Seconds(3)).flatMap(line => {
      Some(line.toString)
    }).foreachRDD({ rdd: RDD[String] =>

      val df = rdd.flatMap(_.split(" ")).toDF.withColumnRenamed("_1", "word")
      val table = df.registerTempTable("words")

      val ans = sqlcontext.sql("select word, count(*) as total from words group by word order by count(*) desc").limit(5).map(x => {
        rank += 1
        (rank, x.getString(0), x.getLong(1))
      })
      rank = 0
      ans.foreachPartition(partitionRecords=>{
        val tablename = "hbase_1102"
        val hbaseconf = HBaseConfiguration.create()
        val conn = ConnectionFactory.createConnection(hbaseconf)
        val tableName = TableName.valueOf(tablename)
        val table = conn.getTable(tableName)
        partitionRecords.foreach(x => {
          val put = new Put(Bytes.toBytes(x._1.toString))
          put.addColumn(Bytes.toBytes("pv1"), Bytes.toBytes("word"), Bytes.toBytes(x._2.toString))
          put.addColumn(Bytes.toBytes("pv1"), Bytes.toBytes("count"), Bytes.toBytes(x._3.toString))
          table.put(put)
        })
        table.close()
//        var jobConf = new JobConf(HBaseConfiguration.create)
//        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "window")
//        //jobConf.setOutputFormat(classOf[TableOutputFormat])//不加这句会报错Undefined job output-path
//        jobConf.setClass("mapreduce.job.outputformat.class", classOf[TableOutputFormat[String]], classOf[OutputFormat[String, Mutation]])
//        //在JobConf中，通常需要关注或者设置五个参数
//        //文件的保存路径、key值的class类型、value值的class类型、RDD的输出格式(OutputFormat)、以及压缩相关的参数
//        ans.map(x => {
//          val put = new Put(Bytes.toBytes(x._1.toString))
//          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("word"), Bytes.toBytes(x._2.toString))
//          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(x._3.toString))
//          (new ImmutableBytesWritable , put)
//        }).saveAsHadoopDataset(jobConf)
        ssc.start()
        ssc.awaitTermination() //等待处理停止,stop()手动停止

      })

    })
  }


}
