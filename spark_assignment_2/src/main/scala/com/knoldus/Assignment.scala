package com.knoldus

import java.sql
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql._
import org.apache.spark.sql.Encoders

class Assignment(path:String) {

  val spark = SparkSession.builder().appName("spark_assignment_2").master("local").getOrCreate()

  import spark.implicits._

  implicit val mapEncoder: Encoder[Map[String, Any]] = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
  implicit val stringIntMapEncoder: Encoder[Map[String, Long]] = ExpressionEncoder()
  implicit val dateEncoder: Encoder[java.sql.Date] = Encoders.DATE

//  val fireRecords: DataFrame = spark.read.csv(path)

  val fireRecords: DataFrame = spark.read.format("com.databricks.spark.csv").option("header", "true").load(path)

  fireRecords.createOrReplaceTempView("fire_records")

  /**
    * Different types of calls made to the Fire Department
    * @return number of calls
    */
  def question1: Long = {

    val countDistinctCallType: DataFrame = fireRecords.select($"Call Type").distinct()
      .filter(x => x(0) != "") //here x(0) is the first column of countDistinctCallType
    countDistinctCallType.count
  }

  /**
    *  incidents of each call type
    *
    * @return An Array of Maps with key as call type and value as its count
    */
  def question2: Array[Map[String, Any]] = {

    val totalCallType: Dataset[Row] = fireRecords.groupBy($"Call Type").count()
      .filter(x => x(0) != "" && x(0) != "Call Type")
    totalCallType.map(_.getValuesMap[Any](List("Call Type", "count"))).collect
  }

  /**
    *
    * @return Total years of Fire Service Calls
    */
  def question3: Long = {

    val result: Dataset[sql.Date] = fireRecords.select("Call Date")
      .filter(x => x(0) != "" ).as[String]
      .map(x => Compute.stringToSqlDate(x))
    val finalResult: Dataset[Int] = result.map(x => x.getYear)
    finalResult.createOrReplaceTempView("date_records")
    spark.sql("select distinct value from date_records").count //value is the new column of finalResult
  }

  /**
    * transforms java.util.Date to java.sql.Date
    * @param date java.util.Date
    * @return java.sql.Date
    */
  private def transform(date: Date): String = {

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")
    dateFormat.format(date)
  }

  /**
    *
    * @return Total service calls logged in the last 7 days
    */
  def question4: Long = {

    val result: Dataset[sql.Date] = fireRecords.select("Call Date")
      .filter(x =>
        x(0) != "").as[String]
      .map(x => Compute.stringToSqlDate(x))
    result.createOrReplaceTempView("last_date_record")
    val lastDate: Option[sql.Date] = spark.sql("select distinct max(value) from last_date_record")
      .as[sql.Date].collect.headOption  //value is the column of result dataset that has all the dates
    val totalServiceCalls: Long = if (lastDate.isDefined) {
      val eighthLastDayString: String = transform(new Date(lastDate.get.getTime - (8 * 24 * 60 * 60 * 1000)))
      val eighthLastDay: sql.Date = Compute.stringToSqlDate(eighthLastDayString)
      result.filter(x => x.after(eighthLastDay) && x.before(lastDate.get)).count
    } else {
      -1
    }
    totalServiceCalls
  }

  /**
    *
    * @return name of the neighbourhood in san fransisco that generated the most calls last year
    */
  def question5: Option[String] = {

    val tempResult: Dataset[sql.Date] = fireRecords.select("Call Date")
      .filter(x => x(0) != "")
      .as[String].map(x => Compute.stringToSqlDate(x))
    tempResult.createOrReplaceTempView("last_date_record")
    val lastDate: Option[sql.Date] = spark.sql("select distinct max(value) from last_date_record")
      .as[sql.Date].collect.headOption  //value is the column of tempResult dataset that has all the dates
    val lastYear: Int = if (lastDate.isDefined) {
      lastDate.get.getYear - 1
    } else {
      -1
    }
    val neighborhoods: DataFrame = fireRecords.filter(x => (x(16) == "San Francisco" || x(16) == "SAN FRANCISCO") && x(4) != "" && x(31) != "")
      .filter(x => Compute.stringToSqlDate(x(4).toString).getYear == lastYear)
      .select($"Neighborhood  District")
    val neighborhoodsWithCount: DataFrame = neighborhoods.groupBy($"Neighborhood  District").count()
    val newNeighborhoodsWithCount: DataFrame =neighborhoodsWithCount.withColumnRenamed("Neighborhood  District","neighborhood_district")
    newNeighborhoodsWithCount.createOrReplaceTempView("neighborhood_record")
    val name: Array[String] =spark.sql("select neighborhood_district from neighborhood_record where count = (select max(count) from neighborhood_record)")
      .as[String].collect()
    name.headOption
  }

}


object Compute{

  val format: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")

  /**
    * converts string type date to java.sql.Date
    * @param strDate date if string
    * @return java.sql.Date
    */
  def stringToSqlDate(strDate:String):java.sql.Date={

    val javaDate=format.parse(strDate)
    new java.sql.Date(javaDate.getTime)
  }

}
