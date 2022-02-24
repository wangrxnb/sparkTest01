package com.wrx.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Work03 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("work03")
        val sc = new SparkContext(sparkConf)

        //1.获取原始数据
        val data = sc.textFile("data/agent.log")
        //2.将原始数据进行转换，==》（（省份，广告），1）
        val mapRDD = data.map(
            line => {
                val splitLine = line.split(" ")
                ((splitLine(1), splitLine(4)), 1)
            }
        )
        //3.分组聚合（（省份，广告），sum）
        val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_+_)
        //4.结构转换（省份，（广告， sum））
        val newMapRDD = reduceRDD.map{
            case ((pro, id), sum) => {
                (pro, (id, sum))
            }
        }

        //5.根据省份进行分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

        //6.降序取前三
        val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )


        //7.打印
        resRDD.collect().foreach(println)

        sc.stop()
    }
}
