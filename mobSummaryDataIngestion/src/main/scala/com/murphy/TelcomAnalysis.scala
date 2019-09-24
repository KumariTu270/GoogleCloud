package com.murphy/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf, when , col, lit, to_date , window , avg, min , max}

object TelcomAnalysis {
  def main(args: Array[String]): Unit =
  {
  val spark = SparkSession.builder.getOrCreate()
  val teleComDF = spark.read.format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .option("header", "false")
    .option("sep", "\t")
    .load("/FileStore/tables/sms_call_internet_tn_2013_11_01-c8c4b.txt")
  import spark.implicits._
    import java.util.Date
    import java.text.SimpleDateFormat

    val epochToDate: UserDefinedFunction = udf((epochMillis : Long) =>{
      val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      df.format(epochMillis)
    })
  val cleanedTelComDF = teleComDF
    .withColumn("_c0", when($"_c0".isNull, 0).otherwise($"_c0"))
    .withColumn("_c1", when($"_c1".isNull, 0).otherwise($"_c1"))
    .withColumn("_c2", when($"_c2".isNull, 0).otherwise($"_c2"))
    .withColumn("_c3", when($"_c3".isNull, 0.0).otherwise($"_c3"))
    .withColumn("_c4", when($"_c4".isNull, 0.0).otherwise($"_c4"))
    .withColumn("_c5", when($"_c5".isNull, 0.0).otherwise($"_c5"))
    .withColumn("_c6", when($"_c6".isNull, 0.0).otherwise($"_c6"))
    .withColumn("_c7", when($"_c7".isNull, 0.0).otherwise($"_c7"))
    .withColumn("time_interval" , lit(10))
    .withColumn("beginning_time" , epochToDate($"_c1"))
    .withColumn("end_time", epochToDate($"_c1".minus(-600000)))
    .select(
      col("_c0").alias("square_id"),
      col("beginning_time").cast("time_stamp"),
      col("end_time").cast("time_stamp"),
      col("time_interval").cast("integer"),
      col("_c2").alias("country_code"),
      col("_c3").alias("SMS_in_activity"),
      col("_c4").alias("SMS_out_activity") ,
      col("_c5").alias("call_in_activity") ,
      col("_c6").alias("call_out_activity"),
      col("_c7").alias("internet_traffic_activity"))

    val tumblingWindowDS = cleanedTelComDF
      .groupBy(window(cleanedTelComDF.col("beginning_time"),"1 hour"))
      .agg(avg($"SMS_in_activity"+$"SMS_out_activity"+$"call_in_activity"+$"call_out_activity"+$"internet_traffic_activity").as("hourly_average"))

    def printWindow(windowDF:DataFrame, aggCol:String): DataFrame = {
      windowDF.sort("window.start").
        select("window.start","window.end",s"$aggCol")
    }
    val windowDF = printWindow(tumblingWindowDS,"hourly_average")
    val peakTime = windowDF.agg(max(windowDF(windowDF.columns(2))).alias("hourly_average")).select("window.start","window.end")
    peakTime.show()
    val hourlyUsageDF = cleanedTelComDF.orderBy("window.start", "square_id")
      .groupBy(window(cleanedTelComDF.col("beginning_time"),"1 hour") , $"square_id")
      .agg(sum($"internet_traffic_activity").as("hourly_internet_usage"))
      .select("window.start" , "window.end" , "square_id" , "hourly_internet_usage")

    cleanedTelComDF.show()

}
}
*/