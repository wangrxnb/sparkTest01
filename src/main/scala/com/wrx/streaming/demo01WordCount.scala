package com.wrx.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object demo01WordCount {
    def main(args: Array[String]): Unit = {
        //创建环境对象
        //StreamingContext创建时需要两个参数
        //参数一：环境变量
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        //参数二：批处理的周期
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //逻辑处理
        //获取端口数据
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val words = lines.flatMap(_.split(" "))
        val wordToOne = words.map((_,1))
        val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_+_)
        wordToCount.print()

        //由于sparkStreaming采集器时长期运行的不能关闭
        //如果main方法执行完毕，应用程序也会结束，所以不能让main方法执行完毕
        //1.启动采集器
        ssc.start()
        //2.等待采集器的关闭
        ssc.awaitTermination()
    }
}
