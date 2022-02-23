package com.wrx.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

//既然是强类型，可能有case类
case class Employee(name:String, salary: Long)
case class Average(var sum:Long, var count: Long)

class myAggFun02 extends Aggregator[Employee, Average, Double] {
  //定义一个数据结构，保存工资总数和工资总个数，初始都为0
  def zero:Average = Average(0L, 0L)

  override def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  //聚合不同execute的结果
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //计算输出
  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble / reduction.count
  }

  //设定之间类型的编码器，要转换成case类
  //Encoders.product 是进行 scala 元组和case类转换的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product

  //设定最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
