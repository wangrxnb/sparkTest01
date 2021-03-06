package com.wrx.work

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object sqlWork {
  def loadData(): Unit ={
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("Hive load data")
      .master("local[*]")
      .getOrCreate()

    spark.sql("use sparkdb")
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |`date` string,
        |`user_id` bigint,
        |`session_id` string,
        |`page_id` bigint,
        |`action_time` string,
        |`search_keyword` string,
        |`click_category_id` bigint,
        |`click_product_id` bigint,
        |`order_category_ids` string,
        |`order_product_ids` string,
        |`pay_category_ids` string,
        |`pay_product_ids` string,
        |`city_id` bigint)
        |row format delimited fields terminated by '\t'
      """.stripMargin)
    spark.sql(
      """
        |load data local inpath 'D:/IDEA_space/sparkTest01/data/user_visit_action.txt' into table sparkdb.user_visit_action
      """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |`product_id` bigint,
        |`product_name` string,
        |`extend_info` string)
        |row format delimited fields terminated by '\t'
      """.stripMargin)
    spark.sql(
      """
        |load data local inpath 'D:/IDEA_space/sparkTest01/data/product_info.txt' into table sparkdb.product_info
      """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |`city_id` bigint,
        |`city_name` string,
        |`area` string)
        |row format delimited fields terminated by '\t'
      """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'D:/IDEA_space/sparkTest01/data/city_info.txt' into table sparkdb.city_info
      """.stripMargin)

    spark.stop()
  }

  def checkSQL(): Unit ={
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("Hive load data")
      .master("local[*]")
      .getOrCreate()

    spark.sql("use sparkdb")
    spark.sql(
      """
        |select
        |	*
        |from(
        |	select
        |		*,
        |		rank() over( partition by area order by clickCnt desc ) as rank
        |	from(
        |		select
        |			area,
        |			product_name,
        |			count(*) as clickCnt
        |		from(
        |			select
        |				u.*,
        |				p.product_name,
        |				c.area,
        |				c.city_name
        |			from user_visit_action u
        |			join product_info p on u.click_product_id = p.product_id
        |			join city_info c on u.city_id = c.city_id
        |			where u.click_product_id > -1
        |		) as t1
        |		group by area,product_name
        |	) as t2
        |) as t3
        |where rank <= 3
      """.stripMargin).show()

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
            .builder()
            .enableHiveSupport()
            .appName("Hive load data")
            .master("local[*]")
            .getOrCreate()

    spark.sql("use sparkdb")

    spark.sql(
      """
        |select
        |	u.*,
        |	p.product_name,
        |	c.area,
        |	c.city_name
        |from user_visit_action u
        |join product_info p on u.click_product_id = p.product_id
        |join city_info c on u.city_id = c.city_id
        |where u.click_product_id > -1
      """.stripMargin).createOrReplaceTempView("t1")

    //?????????????????????????????????cityRemark,?????????spark??????????????????
//    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF()))
    spark.sql(
      """
        |select
        |  area,
        |  product_name,
        |  count(*) as clickCnt
        |  cityRemark(city_name) as city_remark
        |from t1
        |group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")

    //?????????????????????????????????
    spark.sql(
      """
        |select
        |		*,
        |		rank() over( partition by area order by clickCnt desc ) as rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select * from t3 where rank <= 3
      """.stripMargin).show(false)

    spark.stop()
  }

    case class Buffer(var total:Long,var cityMap:mutable.Map[String, Long])
  //???????????????????????????????????????????????????
  //1.??????Aggregator???????????????
  //  IN:????????????
  //  BUF????????????????????????Map[(city,cnt),(city,cnt)]???
  //  OUT???????????????
  //2.????????????
  class CityRemarkUDAF extends Aggregator[String, Buffer, String]{
    //?????????????????????
    override def zero: Buffer = {
      Buffer(0, mutable.Map[String, Long]())
    }

    //?????????????????????
    override def reduce(b: Buffer, city: String): Buffer = {
      b.total += 1
      val newCount = b.cityMap.getOrElse(city, 0L) + 1
      b.cityMap.update(city, newCount)
      b
    }

    //?????????????????????
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total
      val map1 = b1.cityMap
      val map2 = b2.cityMap
      //??????map???????????????
      //?????????
      //      b1.cityMap = map1.foldLeft(map2){
      //        case (map, (city, cnt)) => {
      //          val newCount = map.getOrElse(city, 0L) + cnt
      //          map.update(city, newCount)
      //          map
      //        }
      //      }
      //?????????
      map2.foreach{
        case (city, cnt) => {
          val newCount = map1.getOrElse(city, 0L) + cnt
          map1.update(city, newCount)
        }
      }
      b1.cityMap = map1

      b1
    }

    //???????????????????????????????????????
    override def finish(buff: Buffer): String = {
      val remarkList = ListBuffer[String]()

      val totalcnt = buff.total
      val cityMap  = buff.cityMap

      //????????????
      val cityCountList = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)

      val hasMore = cityMap.size > 2
      var rsum = 0L
      cityCountList.foreach{
        case (city, cnt) => {
          val r = cnt*100 / totalcnt
          remarkList.append(s"${city} ${r}%")
          rsum += r
        }
      }

      if (hasMore){
        remarkList.append(s"?????? ${100-rsum}")
      }

      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
