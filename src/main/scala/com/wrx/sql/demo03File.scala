package com.wrx.sql

import org.apache.spark.sql.SparkSession

object demo03File {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("File test")
      .getOrCreate()

    //方式一
    val df = sparkSession.read.json("hdfs://localhost:9000/tmp/data/people.json")
    df.createOrReplaceTempView("persons")
    df.show()

    //方式二
    val peopleDF = sparkSession.read.format("json").load("hdfs://localhost:9000/tmp/data/people.json")
    peopleDF.write.format("parquet").save("hdfs://localhost:9000/tmp/data/people.parquet")
    peopleDF.show()

    //方式三
    val sqlDF = sparkSession.sql("select * from parquet.`hdfs://localhost:9000/tmp/data/people.parquet`")
    sqlDF.show()
  }
}
