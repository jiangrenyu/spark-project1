package com.jry.spark1;
import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
/**
 * Created by 姜仁雨 on 2017/11/2.
 */
public class SparkStream {
    public static void main(String[] args) {

        // Create a local StreamingContext with two working thread and batch
        // interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount").set("spark.testing.memory","2147480000");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5000));
        System.out.println(jssc);

        // Create a DStream that will connect to hostname:port, like
        // localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop1", 9999);
        //JavaDStream<String> lines = jssc.textFileStream("hdfs://master:9000/stream");

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String x) {
                System.out.println(Arrays.asList(x.split(" ")).get(0));
                return Arrays.asList(x.split(" "));
            }
        });


        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        System.out.println(pairs);
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        // Print the first ten elements of each RDD generated in this DStream to
        // the console

        wordCounts.print();
        //wordCounts.saveAsHadoopFiles("hdfs://master:9000/testFile/", "spark", new Text(), new IntWritable(), JavaPairDStream<Text,IntWritable>());
        wordCounts.dstream().saveAsTextFiles("E:\\系统文件\\开发测试\\tmp\\out\\out1\\oo", "spark");
        //wordCounts.saveAsHadoopFiles("hdfs://master:9000/testFile/", "spark",Text,IntWritable);
        //System.out.println(wordCounts.count());
        jssc.start();
        //System.out.println(wordCounts.count());// Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }

}
