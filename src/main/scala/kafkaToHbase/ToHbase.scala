//package kafkaToHbase
//
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
//import org.apache.hadoop.mapreduce.OutputFormat
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
//import org.apache.hadoop.hbase.{HTableDescriptor,HColumnDescriptor,HBaseConfiguration,TableName}
//import org.apache.hadoop.hbase.client._
//import org.apache.hadoop.mapred.JobConf
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.sql._
//
///**
// * Created by 姜仁雨 on 2017/11/3.
// */
//object ToHbase {
//  def main(args: Array[String]) {
//    val zk = "hadoop1"
//    val group = "1";
//    val topics = "topic6";
//    val numberThreads = "2";
//
//    val sparkConf = new SparkConf().setAppName("local1").setMaster("local[2]")
//    val jsc = new StreamingContext(sparkConf, Seconds(2))
//    jsc.checkpoint("E:\\系统文件\\开发测试\\spark\\ck")
//
//    val topicMap = topics.split(",").map((_, numberThreads.toInt)).toMap
//    val lines = KafkaUtils.createStream(jsc, zk, group, topicMap).map(_._2)
//    val sc = jsc.sparkContext
//    val sqlContext = new SQLContext(sc)
//    lines.foreachRDD(rdd => {
//      import sqlContext.implicits._
//      if(!rdd.isEmpty()){
//        rdd.map(_.split(",")).map(p => Person(p(0),p(1).trim.toDouble,p(2).trim.toInt, p(3).trim.toDouble)).toDF.registerTempTable("TempTable")
//        val rs = sqlContext.sql("select count(1) from TempTable")
//        //Hbase配置部分
//        val hconf = HBaseConfiguration.create()
//        hconf.set("hbase.zookeeper.quorum",zk)
//        hconf.set("hbase.zookeeper.property.clientPort", "2181")
//        hconf.set("hbase.defaults.for.version.skip", "true")
//        hconf.set(TableOutputFormat.OUTPUT_TABLE, "hbase_1102")
////        hconf.setClass("mapreduce.job.outputformat.class", classOf[TableOutputFormat[String]], classOf[OutputFormat[String, Mutation]])
//        val jobConf = new JobConf(hconf)
//        rs.rdd.map(line =>(System.currentTimeMillis(),line(0))).map(line =>{
//          val put = new Put(Bytes.toBytes(line._1))
//          put.addColumn(Bytes.toBytes("pv1"),Bytes.toBytes("pv"),Bytes.toBytes(line._2.toString()))
//          (new ImmutableBytesWritable,put)
//        }).saveAsNewAPIHadoopDataset(jobConf)
//      }
//    })
//
//    jsc.start()
//    jsc.awaitTermination()
//
//  }
//}
//
//case class Person(gender: String, tall: Double, age: Int, driverAge: Double)