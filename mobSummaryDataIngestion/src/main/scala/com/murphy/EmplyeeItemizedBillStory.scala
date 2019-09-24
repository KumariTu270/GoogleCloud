package com.murphy

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

class EmplyeeItemizedBillStory(spark: SparkSession) {

  import spark.implicits._

  def createEmployeeItemized(startYear: Integer, startMonth: Integer, endYear: Integer, endMonth: Integer): Unit = {
    try {
      val schemaItemized = new StructType(Array(new StructField("phone_number", LongType, true),
        StructField("STD_code", StringType, true),
        StructField("start_date", DateType, true),
        StructField("end_date", DateType, true),
        StructField("time", StringType, true),
        StructField("call_type", StringType, true),
        StructField("calling_network", StringType, true),
        StructField("called_network", StringType, true),
        StructField("roaming_operator", StringType, false),
        StructField("incoming_number", StringType, true),
        StructField("outgoing_number", LongType, true),
        StructField("quantity", IntegerType, true),
        StructField("size", StringType, true),
        StructField("duration", StringType, true),
        StructField("period", StringType, true),
        StructField("gross_amount", DoubleType, true),
        StructField("discount", DoubleType, true),
        StructField("total", DoubleType, true),
        StructField("charges_type", StringType, true),
        StructField("billing_month", StringType, false),
        StructField("billing_year", IntegerType, true),
        StructField("operator", StringType, false)

      ))
      var startMon = startMonth
      var startY = startYear
      var itemizedDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaItemized)
      var emplyeeDF = Utils.getEmployeeData(spark)
      if (startMon != 0 && startY != 0) {
        while ((endYear != startY) || (endMonth != startMon)) {
          if (startMonth == 12) {
            startMon = 1
            startY = startY + 1
          }
          else {
            startMon = startMon + 1
          }
          itemizedDF = itemizedDF.union(Utils.getItemizedBillDataByMonth(spark, startMon, startY))
        }
      } else {
        itemizedDF = Utils.getAllItemizedData(spark)
      }
      val joined_df = emplyeeDF.join(itemizedDF, emplyeeDF.col("contact_number") === itemizedDF.col("phone_number"), "inner")

      val final_df = joined_df.select($"phone_number", $"username", $"location", $"cost_center", $"department", $"supervisor", $"head_of_department"
        , $"operator", $"start_date", $"end_date", $"time", $"duration", $"quantity", $"size", $"call_type", $"type",
        $"incoming_number", $"outgoing_number", $"gross_amount", $"discount", $"total", $"charges_type", $"billing_month",
        $"billing_year")
      addEmployeeItemizedToHive(final_df)
    } catch {
      case e: NullPointerException => throw new NullPointerException("employeeitemized bill story addToHive() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def addEmployeeItemizedToHive(data: DataFrame): Unit = {
    try {
      data.write.format("orc").mode(SaveMode.Append).saveAsTable("mob_bill_summary_uz" + "." + "employee_itemized_bill")
      println("Table added")
    } catch {
      case e: NullPointerException => throw new NullPointerException("employeeitemized bill story addToHive() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

}
