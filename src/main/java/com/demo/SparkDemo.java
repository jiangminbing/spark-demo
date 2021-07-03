package com.demo;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        JavaPairRDD<Object, Object> rdd2 = rdd.flatMap(line -> Arrays.asList(line.split("\\|")).iterator())
                .mapToPair(d->new Tuple2<>(d,d));
        /**
         * 现在有10个分区，共1000条数据，假设每个分区的数据=1000/10=100条，分别使用map和mapPartition遍历。
         * (1)、使用map(func())遍历
         * 现在，当我们将map（func）方法应用于rdd时，func（）操作将应用于每一行，在这种情况下，func（）操作将被调用1000次。即在一些时间关键的应用中会耗费时间。
         *(2)、使用mapPartition(func())遍历
         * 如果我们在rdd上调用mapPartition（func）方法，则func（）操作将在每个分区上而不是在每一行上调用。在这种特殊情况下，它将被称为10次（分区数）。通过这种方式，你可以在涉及时间关键的应用程序时阻止一些处理。\
         */
        JavaRDD<Integer> rdd3 = sc.parallelize(Arrays.asList(1,2,4));
        JavaRDD<Integer> rdd4 = rdd3.mapPartitions(d->{
          List<Integer> r = new ArrayList();
          while (d.hasNext()) {
              int i = d.next();
              r.add(i * 2);
          }
          return r.iterator();
        });



        sc.stop();

    }


}
