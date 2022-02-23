package com.wrx.sql

import org.apache.spark.sql.SparkSession

object demo04 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("Hive")
      .master("local[*]")
      .getOrCreate()

//    sparkSession.sql("create database sparkdb")
    sparkSession.sql("use sparkdb")
//    sparkSession.sql("create table sparkdb.testdb(id int)")
    sparkSession.sql("load data local inpath 'D:/IDEA_space/sparkTest01/data/index.txt' into table sparkdb.testdb")
    sparkSession.sql("select * from sparkdb.testdb")
    sparkSession.sql("show tables").show()

    /*
    1.创建数据库
    2.use 数据库
    3.创建表
    4.加载数据
     */

    sparkSession.stop()
  }
}
