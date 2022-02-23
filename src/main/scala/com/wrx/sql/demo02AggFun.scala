package com.wrx.sql

import org.apache.spark.sql.SparkSession

object demo02AggFun {
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
    val df = sparkSession.read.json("hdfs://localhost:9000/tmp/data/salary.json")
    val ds = df.as[Employee]
    df.createOrReplaceTempView("employees")
    df.show()

    //注册函数
    sparkSession.udf.register("myAverage", new myAggFun01)
    val result = sparkSession.sql("select myAverage(salary) as average_salary from employees")
    result.show()

    val myAgg = new myAggFun02()
    val averageSalary = myAgg.toColumn.name("average_salary")
    val result02 = ds.select(averageSalary)
    result02.show()
  }
}
