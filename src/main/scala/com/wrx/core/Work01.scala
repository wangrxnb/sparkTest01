package com.wrx.core

import org.apache.spark.{SparkConf, SparkContext}

object Work01 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Work01")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.textFile("data/apache.log")
        val url = rdd.map(
            line => {
                val datas = line.split(" ")
                datas(6)
            }
        )
        url.collect().foreach(println)
}
}
