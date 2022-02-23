package com.wrx.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import scala.collection.mutable

object demo02Queue {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(4))

        val rddQueue = new mutable.Queue[RDD[Int]]()
        val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
        val mappedStream = inputStream.map((_,1))
        val reducesStream = mappedStream.reduceByKey(_+_)

        reducesStream.print()

        ssc.start()

        for(i <- 1 to 50){
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }

        ssc.awaitTermination()
    }
}
