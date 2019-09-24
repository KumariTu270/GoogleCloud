package com.murphy

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

class EmployeeMonthlyTopTable(val spark: SparkSession) {
  def ingestEmployeeMonthlyTopTable(startYear: Integer, startMonth: Integer, endYear: Integer, endMonth: Integer): Unit = {
    import spark.implicits._
    val schema = new StructType(Array(new StructField("phone_number", LongType, true),
      StructField("username", StringType, true),
      StructField("location", StringType, true),
      StructField("cost_center", StringType, true),
      StructField("department", StringType, true),
      StructField("supervisor", StringType, true),
      StructField("head_of_department", StringType, true),
      StructField("operator", StringType, false),
      StructField("base_charges", DoubleType, true),
      StructField("value_added_service", DoubleType, false),
      StructField("usage_charges", DoubleType, true),
      StructField("other_charges", DoubleType, true),
      StructField("discount", DoubleType, true),
      StructField("total", DoubleType, true),
      StructField("billing_month", StringType, false),
      StructField("billing_year", IntegerType, true)
    ))

    val schemaMonthly = new StructType(Array(StructField("operator", StringType, false), StructField("phone_number", LongType, true),
      StructField("STD_code", StringType, true), StructField("account_number", LongType, true),
      StructField("employee_account_number", LongType, true),
      StructField("credit_limit", DoubleType, true),
      StructField("base_charges", DoubleType, true),
      StructField("value_added_service", DoubleType, false),
      StructField("usage_charges", DoubleType, true),
      StructField("other_charges", DoubleType, true),
      StructField("discount", DoubleType, true),
      StructField("total", DoubleType, true), StructField("billing_month", StringType, false),
      StructField("billing_year", IntegerType, false)
    ))
    var startMon = startMonth
    var startY = startYear
    val dfEmployee = spark.sqlContext.sql("select * from mob_bill_summary_uz.employee_master")
    var dfMonthly = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaMonthly)
    //var dfMonthly=spark.sqlContext.sql("select * from mob_bill_summary_uz.monthlybillsummary")
    if (startMon != 0 && startY != 0) {
      while ((endMonth != startMon) && (endYear != startY)) {
        if (startMonth == 11) {
          startMon = 1
          startY = startY + 1
        }
        else {
          startMon = startMon + 1
        }
        dfMonthly.union(Utils.getMonthlyDatabyMonth(spark, startMon, startY))
      }
    } else {
      dfMonthly = Utils.getAllMonthlyData(spark)
    }

    val joined_df = dfEmployee.join(dfMonthly, dfEmployee.col("contact_number") === dfMonthly.col("phone_number"), "inner")

    val final_df = joined_df.distinct().select($"phone_number", $"username", $"location",
      $"cost_center", $"department", $"supervisor", $"head_of_department",
      $"operator", $"base_charges", $"value_added_service", $"usage_charges",
      $"other_charges", $"discount", $"total", $"billing_month",
      $"billing_year")

    Utils.addEmplyeeMonthlyTopTableToHive(final_df)
  }
}
