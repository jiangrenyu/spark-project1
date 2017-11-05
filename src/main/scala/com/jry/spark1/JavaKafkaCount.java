package com.jry.spark1;


import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;


import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;


/**
 * Created by 姜仁雨 on 2017/11/2.
 */
public class JavaKafkaCount {
    private static final Pattern SPACE = Pattern.compile(",");

    public static  void main(String args[]) throws InterruptedException {
        if(args.length<4){
            System.out.println("输入参数不正确");
            System.exit(-1);
        }
        SparkConf conf = new SparkConf().setAppName("kafka-count").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf,new Duration(2));
        int numberTreads = Integer.parseInt(args[3]);
        Map<String,Integer> topicMap = new HashMap<String, Integer>();
        String topics [] = args[2].split(",");
        for(String topic : topics){
            topicMap.put(topic,numberTreads);
        }
        //Kafka消息
        //Kafka消息分为
        //接收Kafka的消息
        JavaPairReceiverInputDStream<String,String> messages = KafkaUtils.createStream(jsc,args[0],args[1],topicMap);
        //对Kafka消息进行map操作
        JavaDStream <String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2();//获取Kafka的消息body,_1就是获取Kafka消息的head
            }
        });

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {//两个String分别代表输入和输出

            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s));
                //return newArrayList(SPACE.split(s));
            }
        });

        JavaPairDStream<String,Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1+i2;
            }
        });
        wordCounts.print();
        jsc.start();
        jsc.awaitTermination();

    }
}
