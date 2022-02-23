package com.wrx.core

import org.apache.spark.{SparkConf, SparkContext}

object Work02 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Work01")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(1,2,3,4,5,6,7,8,9,10,11,12),
            3
        )
        val maxPar = rdd.mapPartitions(
            datas => {
                List(datas.max).iterator
            }
        )
        maxPar.collect().foreach(println)

    }
}
