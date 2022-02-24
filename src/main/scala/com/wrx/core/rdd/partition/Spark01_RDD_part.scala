package com.wrx.core.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_part {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark partitions")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("nba","*******"),
            ("cba","*******"),
            ("wba","*******"),
            ("nba","*******")
        ), 3)

        val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

        partRDD.saveAsTextFile("output")
    }

    class MyPartitioner() extends Partitioner{
        //分区的数量
        override def numPartitions: Int = 3

        //根据数据的key返回数据所在的分区索引，从0开始
        override def getPartition(key: Any): Int = {
            key match {
                case "nba" => 0
                case "cba" => 1
                case _ => 2
            }
        }
    }
}
