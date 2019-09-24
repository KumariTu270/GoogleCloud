package com.murphy

import java.io._

import org.apache.spark.sql.functions.{col, lit, to_date, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class ItemizedBIllStory(val spark: SparkSession) {

  import spark.implicits._

  def ingestItemizedBill(sourcePath: String, destinationPath: String): Unit = {
    try {
      val folderName: String = "Maxis_data/Itemized_bill/"
      println(sourcePath + folderName)
      val fileList: List[String] = Utils.getFileList(sourcePath + folderName)
      if (fileList.isEmpty) {
        println("no Maxis itemized files present in folder")
      }
      else {
        addDataToHive(fileList, spark, destinationPath)
      }
    } catch {
      case e: FileNotFoundException => throw new FileNotFoundException("Itemized bill file not found")
      case e: NullPointerException => throw new NullPointerException("problem in loading data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def addDataToHive(fileList: List[String], spark: SparkSession, destinationPath: String) = {
    try {
      for (i <- fileList.indices) {
        val cleanedDF = loadAndCleanItemizedData(fileList(i), spark)
        val finalDF = transformItemizedData(cleanedDF, spark)
        val numberOfRows = finalDF.collect().length
        //finalDF.show()
        addToHive(finalDF)
        println("In Itemized")
        Utils.moveFileToArchive(fileList(i), destinationPath)
      }
    } catch {
      case e: FileNotFoundException => throw new FileNotFoundException("Itemized bill story addDataTOHive() file not found")
      case e: NullPointerException => throw new NullPointerException("itemized bill story addDataTOHive() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def transformItemizedData(cleanedDF: DataFrame, spark: SparkSession): DataFrame = {
    try {
      val finalDF = cleanedDF.select(
        col("Calling_Number").alias("phone_number").cast("long")
        , col("STD_code").alias("STD_code").cast("String")
        ,to_date(unix_timestamp(col("Start_Date"), "dd/MM/yy").cast("timestamp")).alias("start_date").cast("date")
        ,to_date(unix_timestamp(col("End_Date"), "dd/MM/yy").cast("timestamp")).alias("end_date").cast("date")
//        , to_date(col("Start_Date"), "dd/MM/yy").alias("start_date").cast("date")
//        , to_date(col("End_Date"), "dd/MM/yy").alias("end_date").cast("date")
        , col("Time").alias("time").cast("String")
        , col("Type").alias("call_type").cast("String")
        , col("From").alias("calling_network").cast("String")
        , col("To").alias("called_network").cast("String")
        , col("Operator").alias("roaming_operator").cast("String")
        , col("PhoneNumber").alias("incoming_number").cast("String")
        , col("NumberCalled").alias("outgoing_number").cast("Long")
        , col("Quantity").alias("quantity").cast("Integer")
        , col("Size").alias("size").cast("String")
        , col("Duration").alias("duration").cast("String")
        , col("Period").alias("period").cast("String")
        , col("GrossAmount").alias("gross_amount").cast("Double")
        , col("Discount").alias("discount").cast("Double")
        , col("Total").alias("total").cast("Double")
        , col("Call Type").alias("charges_type").cast("String")
        , col("Billing_Month").alias("billing_month").cast("String")
        , col("Billing_Year").alias("billing_year").cast("Integer")
        , col("operator").alias("operator").cast("String")
      )
      return finalDF
    } catch {
      case e: NullPointerException => throw new NullPointerException("Datafame Null in transformItemizedData")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def loadAndCleanItemizedData(file: String, spark: SparkSession): DataFrame = {
    try {
      val df = spark.read.option("header", "true").csv(file)
      val fileName = file
      val monthName = getMonthNameItemized(fileName)
      val year = getYearItemized(fileName)
      val cleanedDF = df.select(
        col("Calling Number")
        , col("Date")
        , col("Time")
        , col("Type")
        , col("From")
        , col("To")
        , col("Operator")
        , col("PhoneNumber")
        , Utils.removeStringItemized(col("NumberCalled")).alias("NumberCalled")
        , col("Quantity")
        , col("Size")
        , col("Duration")
        , Utils.removeHyphenItemized(col("Period")).alias("Period")
        , col("GrossAmount")
        , col("Discount")
        , col("Total")
        , col("Call Type"))
        .withColumn("STD_code", Utils.getCodeItemized($"Calling Number"))
        .withColumn("Calling_Number", Utils.removeStdCodeItemized($"Calling Number"))
        .withColumn("End_Date", Utils.getEndDateItemized(df.col("Date")))
        .withColumn("Start_Date", Utils.removeEndDateItemized($"Date"))
        .withColumn("Billing_Month", lit(monthName))
        .withColumn("Billing_Year", lit(year))
        .withColumn("operator", lit("MAXIS"))
      cleanedDF
    } catch {
      case e: FileNotFoundException => throw new FileNotFoundException("Itemized bill story loadAndClean() file not found")
      case e: NullPointerException => throw new NullPointerException("itemized bill story loadAndClean() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def getYearItemized(path: String): String = {
    try {
      val nameArr = path.split("/")
      val name = nameArr(nameArr.length - 1)
      val year = name.substring(0, 4)
      year
    } catch {
      case e: NullPointerException => throw new NullPointerException("Utils class getYearItemized() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def getMonthNameItemized(name: String): String = {
    try {
      val arr = name.split("/")
      val filename = arr(arr.length - 1)
      val month = Integer.parseInt(filename.substring(4, 6))
      month match {
        case 1 => "January"
        case 2 => "February"
        case 3 => "March"
        case 4 => "April"
        case 5 => "May"
        case 6 => "June"
        case 7 => "July"
        case 8 => "August"
        case 9 => "September"
        case 10 => "October"
        case 11 => "November"
        case 12 => "December"
        case _ => null

      }
    } catch {
      case e: NullPointerException => throw new NullPointerException("Utils class getMonthNameItemized() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def addToHive(dataframe: DataFrame): Unit = {
    try {
      dataframe.write.format("orc").mode(SaveMode.Append).saveAsTable("mob_bill_summary_uz" + "." + "Itemized_Bill")
    } catch {
      case e: NullPointerException => throw new NullPointerException("itemized bill story addToHive() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }


}
