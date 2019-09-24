package com.murphy

import java.io.FileNotFoundException

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class CelcomMonthlyBillStory(val sparkSession: SparkSession) {

  def getMonthlyCelcomBill(path: String, destPath: String): Unit = {

    try {

      val folderPath = "CelcomData/"
      val fileList = Utils.getFileList(path + folderPath)
      if (fileList.isEmpty) {
        println("no Celcom monthly bill files present in folder")
      } else {
        fileList.foreach(println)
        for (i <- fileList.indices) {
          val fileObj = fileList(i)
          val pathOfFile = fileObj
          loadData(pathOfFile)
          println("moving file Celcom :"+ pathOfFile)
          println("In Celcom Story")
          Utils.moveFileToArchive(pathOfFile, destPath)

        }
      }
    } catch {
      case ex: FileNotFoundException => throw new FileNotFoundException(ex.getMessage)
      case ex: NullPointerException => throw new NullPointerException("NullPointerException")
      case ex: IllegalArgumentException => throw new IllegalArgumentException("Illegal Argument")
      case ex: Exception => throw new Exception(ex.getMessage)
    }

    def loadData(pathOfFile: String) {

      try {
        val celcomEmployeeDataFrame: DataFrame = sparkSession.sqlContext.read
          .format("com.crealytics.spark.excel")
          .option("useHeader", "true")
          .option("inferSchema", "false")
          .option("startColumn", 1)
          .option("endColumn", 99)
          .option("treatEmptyValuesAsNulls", "true")
          .option("skipFirstRows", 14)
          .load(pathOfFile)

        val celcomDateDataFrame: DataFrame = sparkSession.sqlContext.read
          .format("com.crealytics.spark.excel")
          .option("useHeader", "false")
          .option("inferSchema", "false")
          .option("startColumn", 2)
          .option("endColumn", 2)
          .option("skipFirstRows", 4)
          .option("treatEmptyValuesAsNulls", "true")
          .option("location", pathOfFile)
          .load(pathOfFile)


        val data4 = celcomDateDataFrame.select(col("_c0").alias("date").cast("String")).head()
        val format = new java.text.SimpleDateFormat("dd MMM yyyy")
        val date = data4.toString().replace("[", "").replace("]", "")
        val data3date = format.parse(date)
        val year = data3date.getYear
        val month = Utils.getMonthName(data3date.getMonth)
        val year1 = year + 1900
        cleanDataFrame(celcomEmployeeDataFrame, month, year1)
      }
      catch {
        case ex: FileNotFoundException => throw new FileNotFoundException("Celcom Employee File Not Found")
      }
    }


    def cleanDataFrame(celcomEmployeeDataFrame: DataFrame, month: String, year1: Int) {
      try {
        import sparkSession.implicits._
        val data = celcomEmployeeDataFrame.filter($"Account No".isNotNull && $"Credit Limit".isNotNull).select(col("Mobile No").alias("phone_no")
          , col("Account No").alias("account_number").cast("long")
          , col("Child Account No").alias("employee_account_number").cast("long")
          , col("Credit Limit").alias("credit_limit").cast("double")
          , col("Monthly Charges").alias("base_charges").cast("double")
          , col("Usage Call/Charges").alias("usage_charges").cast("double")
          , col("One Time Charges").alias("other_charges").cast("double")
          , col("Discount and Rebates").alias("discount").cast("double")
          , col("Current Charges (Excl Tax)").alias("total").cast("double"))
          .withColumn("STD_code", Utils.extractCodeCelcom(col("phone_no")))
          .withColumn("phone_number", Utils.tailCodeCelcom($"phone_no"))
          .withColumn("value_added_service", lit(Double.NaN))
          .withColumn("billing_month", lit(month))
          .withColumn("billing_year", lit(year1))
          .withColumn("operator", lit("CELCOM"))
        filterDataFrame(data)
      } catch {
        case ex: NullPointerException => throw new NullPointerException("NullPointerException")
        case ex: IllegalArgumentException => throw new IllegalArgumentException("Illegal Argument")

      }
    }

    def filterDataFrame(data: DataFrame) {

      val finalCelcomDataFrame = data.select(col("operator").cast("String"), col("phone_number").cast("long"), col("STD_code"), col("account_number")
        , col("employee_account_number")
        , col("credit_limit"), col("base_charges")
        , col("value_added_service"), col("usage_charges")
        , col("other_charges"), col("discount")
        , col("total"), col("billing_month"), col("billing_year"))

      addCelcomMonthlyDataToHive(finalCelcomDataFrame)
    }

    def addCelcomMonthlyDataToHive(finalCelcomDataFrame: DataFrame) {

      val numberOfRows = finalCelcomDataFrame.collect().length
      //finalCelcomDataFrame.show(numberOfRows)
      Utils.addMonthlyToHive(finalCelcomDataFrame)
    }
  }


}


//    catch{
//      case ex: FileNotFoundException => throw new FileNotFoundException(ex.getMessage)
//      case ex: NullPointerException => throw new NullPointerException("NullPointerException")
//      case ex: IllegalArgumentException => throw new IllegalArgumentException("Illegal Argument")
//      case ex: Exception => throw new Exception(ex.getMessage)
//
//    }





