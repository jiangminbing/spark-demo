package com.demo;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021/6/26 15:25
 */
public class SparkDemo {
    public static void main(String[] args) {
        String appName = "demo";
        SparkConf conf = new SparkConf().
                setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 获取一个分布式数据集合
        JavaRDD<String> rdd = sc.textFile("hdfs://10.2.1.88:4007/test/countfile");
        // 聚合求和
//        int count = rdd.map(line-> Arrays.stream(line.split("\\|"))
//                .map(Integer::valueOf).reduce((a,b)->(a+b)).get()).reduce((a,b)->a+b);
//        System.out.println(count);
//        JavaRDD<Integer> rdd1 = rdd.map(line -> {
//            int count = Arrays.stream(line.split("\\|")).map(Integer::valueOf).reduce((a, b) -> a + b).get();
//            return count;
//        });
//        rdd1.saveAsObjectFile("hdfs://10.2.1.88:4007/test/countresult");
        // 数据集迭代
        JavaRDD<String> rdd1 = rdd.flatMap(line -> Arrays.asList(line.split("\\|")).iterator());
        // 数据集元组转换
        JavaRDD<String,String> rdd2 = rdd.flatMap(line -> Arrays.asList(line.split("\\|")).iterator()).mapToPair(d->new Tuple2<>(d.toString(),d));
        sc.stop();

    }


}
