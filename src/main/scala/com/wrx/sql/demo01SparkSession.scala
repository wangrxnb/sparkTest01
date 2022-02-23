package com.wrx.sql

import org.apache.spark.sql.SparkSession

object demo01SparkSession {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSession test")
      .master("local[*]")
      .getOrCreate()

    //导入隐式转换
    import sparkSession.implicits._

    //读取本地文件，创建DF
    val df = sparkSession.read.json("hdfs://localhost:9000/tmp/data/people.json")

    //打印
    df.show()

    //DSL风格：查询年龄在20岁以上的
    df.filter($"age">20).show()

    //创建临时表
    df.createOrReplaceTempView("persons")

    //SQL风格：查询年龄在20岁以上的
    sparkSession.sql("select * from persons where age>20").show()

    //关闭连接
    sparkSession.stop()
  }
}
