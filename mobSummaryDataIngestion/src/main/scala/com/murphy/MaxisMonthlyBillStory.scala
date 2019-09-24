package com.murphy

import java.io.FileNotFoundException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class MaxisMonthlyBillStory(val spark: SparkSession) {

  import spark.implicits._

  def getMonthlyBill(sourcePath: String, destinationPath: String): Unit = {
    try {
      val folderPath = "Maxis_data/MonthlyBillData/"
      val fileList = Utils.getFileList(sourcePath + folderPath)
      if (fileList.isEmpty) {
        println("no Maxis monthly bill files present in folder")
      }
      else {
        addDataToHive(fileList, spark, destinationPath)
      }
    } catch {
      case fileNotFound: FileNotFoundException => throw new FileNotFoundException("MaxisMonthlyFileNotFoundException")
      case nullException: NullPointerException => throw new NullPointerException("NullPointerException")
      case ex: Exception => println(ex.printStackTrace())
    }
  }

  def addDataToHive(fileList: List[String], session: SparkSession, destinationPath: String): Unit = {
    try {
      for (i <- fileList.indices) {
        val dataFrameAccountSummary = loadData(fileList(i), "Account Summary", 1, 12, 6)
        val dataFrameDate = loadData(fileList(i), "Account Summary", 3, 4, 0)
        val dataFrameAccountNumber = loadData(fileList(i), "Tax Invoice", 6, 7, 6)
        val cleanedData = cleanData(dataFrameAccountSummary)
        val filteredData = filterData(cleanedData)
        val finalData = showData(filteredData, dataFrameAccountNumber, dataFrameDate)
        Utils.addMonthlyToHive(finalData)
        Utils.moveFileToArchive(fileList(i), destinationPath)
      }
    } catch {
      case nullException: NullPointerException => throw new NullPointerException("NullPointerException")
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  def loadData(file: String, sheetName: String, startColumn: Int, endColumn: Int, skipFirstRows: Int): DataFrame = {
    try {
      val dataFrame = spark.sqlContext.read
        .format("com.crealytics.spark.excel")
        .option("sheetName", sheetName)
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "false")
        .option("startColumn", startColumn)
        .option("endColumn", endColumn)
        .option("skipFirstRows", skipFirstRows)
        .load(file)
      return dataFrame

    } catch {
      case fileNotFound: FileNotFoundException => throw new FileNotFoundException("MaxisMonthlyFileNotFoundException")
      case nullException: NullPointerException => throw new NullPointerException("NullPointerException")
      case ex: Exception => throw new Exception(ex.getMessage)
    }

  }

  def cleanData(dataFrameAccountSummary: DataFrame): DataFrame = {
    try {
      val cleanedDF = dataFrameAccountSummary.filter($"_c2".isNotNull && $"_c3".isNotNull && $"_c11".isNotNull).select(col("_c0").alias("contact_number").cast("string"),
        col("_c3").alias("base_charges").cast("double"),
        col("_c4").alias("value_added_service").cast("double"),
        col("_c5").alias("usage_charges").cast("double"),
        col("_c6").alias("other_charges").cast("double"),
        col("_c7").alias("discount").cast("double"),
        col("_c11").alias("total").cast("double"))
      cleanedDF
    } catch {
      case nullException: NullPointerException => throw new NullPointerException("NullPointerException")
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  def filterData(cleanedDF: DataFrame): DataFrame = {
    try {
      val filteredDF = cleanedDF.filter($"base_charges".isNotNull)
        .withColumn("employee_account_number", lit(null).cast("long"))
        .withColumn("credit_limit", lit(null).cast("double"))
        .withColumn("STD_code", Utils.extractCodeMaxisMonthly($"contact_number").cast("string"))
        .withColumn("phone_number", Utils.tailCodeMaxisMonthly($"contact_number").cast("long"))
        .withColumn("operator", lit("MAXIS"))
      filteredDF
    } catch {
      case nullException: NullPointerException => throw new NullPointerException("NullPointerException")
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  def showData(filteredDF: DataFrame, dataFrameAccountNumber: DataFrame, dataFrameDate: DataFrame): DataFrame = {
    try {
      val showData = filteredDF.select(
        col("operator"),
        col("phone_number"),
        col("STD_code"),
        col("employee_account_number"),
        col("credit_limit"),
        col("base_charges"),
        col("value_added_service"),
        col("usage_charges"),
        col("other_charges"),
        col("discount"),
        col("total"))
        .withColumn("account_number", lit(getAccountNumber(dataFrameAccountNumber)))
        .withColumn("billing_month", lit(getDate(dataFrameDate)(0)))
        .withColumn("billing_year", lit(Integer.parseInt(getDate(dataFrameDate)(1))))


      val final_data = showData.select(
        col("operator").cast("string"),
        col("phone_number").cast("long"),
        col("STD_code").cast("string"),
        col("account_number").cast("long"),
        col("employee_account_number").cast("long"),
        col("credit_limit").cast("double"),
        col("base_charges").cast("double"),
        col("value_added_service").cast("double"),
        col("usage_charges").cast("double"),
        col("other_charges").cast("double"),
        col("discount").cast("double"),
        col("billing_month").cast("string"),
        col("total").cast("double"),
        col("billing_year").cast("integer"))
      final_data
    } catch {
      case nullException: NullPointerException => throw new NullPointerException("NullPointerException")
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  def getAccountNumber(dataFrameTaxInvoice: DataFrame): String = {
    val accountDF = dataFrameTaxInvoice.select(col("_c0").cast("string").substr(4, 9).alias("account_number"))
      .withColumn("operator", lit("MAXIS").cast("string"))
    val account_number = accountDF.select(col("account_number")).collect().map(_.toString().replace("[", "").replace("]", "")).head

    account_number
  }

  def getDate(dataFrameDate: DataFrame): List[String] = {
    val dateDF = dataFrameDate.select(col("_c0").substr(1, 10).alias("date")).head()
    val format = new java.text.SimpleDateFormat("dd/MM/yyyy")
    val date = dateDF.toString().replace("[", "").replace("]", "")
    val dateFormatDF = format.parse(date)
    val month = Utils.getMonthName(dateFormatDF.getMonth)
    val year = dateFormatDF.getYear() + 1900
    var monthYear = List(month, year.toString)
    println(monthYear)
    return monthYear
  }

  def getMaxisMonthlyBillFileList(path: String): List[String] = {
    val uri = new URI("hdfs://devmurphy/")
    val fs = FileSystem.get(uri, new Configuration())
    val filePath = new Path(path)
    val files = fs.listFiles(filePath, false)
    var fileList = List[String]()
    while (files.hasNext) {
      fileList ::= files.next().getPath().toString
    }
    fileList

  }


}

