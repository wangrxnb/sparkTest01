package com.wrx.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Partitions {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Partition")
        val sparkContext = new SparkContext(sparkConf)
        val rdd = sparkContext.makeRDD(
            List(1,2,3)
        )
        rdd.saveAsTextFile("output")
        sparkContext.stop()
    }

}
