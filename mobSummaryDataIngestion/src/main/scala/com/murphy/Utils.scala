package com.murphy

import java.io.{File, FileNotFoundException}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object Utils {
  val function:UserDefinedFunction = udf(
    (x:Int) => {x+1}
  )

  val removeHyphenItemized:UserDefinedFunction = udf((period: String) => {
    if (period != null && period.contains("-"))
      null
    else
      period
  })
  val removeStringItemized:UserDefinedFunction = udf((number: String) => {
    if (number != null)
      number.replaceAll("[^0-9]", "")
    else
      number
  })
  val getCodeItemized :UserDefinedFunction = udf((number: String) => {
    if (number != null && number.contains("(") && number.contains(")")) {
      val code = number.split("\\)")
      code(0).replaceAll("\\(", "")
    }
    else {
      null
    }
  })
  val getEndDateItemized = udf((date: String) => {
    if (date != null && date.contains("-")) {
      val dates = date.split("-")
      dates(1)
    }
    else {
      null
    }
  })
  val removeStdCodeItemized = udf((number: String) => {
    if (number != null && number.contains("(") && number.contains(")")) {
      val code = number.split("\\)")
      code(1)
    }
    else {
      number
    }
  })
  val removeEndDateItemized = udf((date: String) => {
    if (date != null && date.contains("-")) {
      val dates = date.split("-")
      dates(0)
    }
    else {
      date
    }
  })
  val extractCodeMaxisMonthly = udf(f = (contact_number: String) => {
    if (contact_number.contains("(") && contact_number.contains(")")) {
      val code = contact_number.split("\\)")
      code(0).replace("(", "")
    } else {
      null
    }
  })
  val extractCodeCelcom: UserDefinedFunction = udf((phone_number: String) => {
    if (phone_number != null && phone_number.contains("-")) {
      val code = phone_number.split("-").head
      code
    }
    else {
      null
    }
  })
  val tailCodeCelcom: UserDefinedFunction = udf((phone_number: String) => {
    if (phone_number != null && phone_number.contains("-")) {
      val code1 = phone_number.split("-")(1)
      val code2 = code1.substring(0, 7)
      code2
    } else {
      null
    }
  })
  val tailCodeMaxisMonthly = udf(f = (contact_number: String) => {
    if (contact_number != null && contact_number.contains("(") && contact_number.contains(")")) {
      val code = contact_number.split("\\)")
      code(1)
    } else {
      null
    }
  })


  val extract_std_code: UserDefinedFunction = udf((phone_number: String) => {
    if (phone_number != null && phone_number.contains("-"))
      phone_number.split("-").head
    else
      "null"
  })
  val removeBracketEmployeeData = udf((phone_number: String) => {
    if (phone_number != null && (phone_number.contains("(") || phone_number.contains(")"))) {
      phone_number.replace("(", "").replace(")", "")
    } else {
      phone_number
    }
  })
  val removeHyphenEmployeeData: UserDefinedFunction = udf((phone_number: String) => {
    if (phone_number != null && phone_number.contains("-")) {
      phone_number.replace("-", "")
    }
    else {
      phone_number
    }
  })

  def getFileList(path: String): List[String] = {
    val uri = new URI("hdfs://devmurphy/")
    val fs = FileSystem.get(uri, new Configuration())
    val filePath = new Path(path)
    val files = fs.listFiles(filePath, false)
    var fileList = List[String]()

    while (files.hasNext) {
      fileList ::= files.next().getPath.toString
    }
    fileList
  }

  def moveFileToArchive(inPath: String, toPath: String): Unit = {
    val uri = new URI("hdfs://devmurphy/")
    val file = inPath.split("/")
    val filename = file(file.length - 1)
    val fs = FileSystem.get(uri, new Configuration())
    val sourcePath = new Path(inPath)
    val destPath = new Path(toPath + "/" + filename)
    println(sourcePath)
    println(destPath)
    fs.rename(sourcePath, destPath)
   // println("moved")
  }

  def addMonthlyToHive(dataframe: DataFrame): Unit = {
    dataframe.write.format("orc").mode(SaveMode.Append).saveAsTable("mob_bill_summary_uz" + "." + "monthly_bill_summary")
  }

  def addEmplyeeDataToHive(dataframe: DataFrame): Unit = {
    dataframe.write.format("orc").mode(SaveMode.Append).saveAsTable("mob_bill_summary_uz" + "." + "employee_master")
  }

  def getCelcomMonthlyBillFileList(path: String): List[File] = {
    val dir = new File(path)
    if (dir.exists() && dir.isDirectory) {
      dir.listFiles().toList
    }
    else if (!dir.exists()) {
      throw new FileNotFoundException("Enter a proper path for Itemized bill")
    }
    else {
      List[File]()
    }
  }

  def getMaxYearFromItemizedbill(spark: SparkSession): Integer = {
    import spark.implicits._
    try {
      var year = 0
      if (spark.sqlContext.sql("show tables in mob_bill_summary_uz like 'itemized_bill'").collect().length >= 1) {
        val yearCol = spark.sqlContext.sql("select max(billing_year) from mob_bill_summary_uz.itemized_bill")
        val yearNum: Array[Integer] = yearCol.as[Integer].collect()
        year = yearNum(0)
      }
      year
    } catch {
      case e: NullPointerException => throw new NullPointerException("Utils class getyearfromItemized() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }

  }

  def getMaxMonthFromItemizedbill(spark: SparkSession, year: String) = {
    try {
      var monthMax: Integer = 0
      if (spark.sqlContext.sql("show tables in mob_bill_summary_uz like 'itemized_bill'").collect().length == 1) {
        val months = getMonthsFromItemizedbill(spark, year)
        val monthsDistinct = months.distinct().collect().map(_ (0).toString).toArray

        for (month <- monthsDistinct) {
          if (monthMax < getMonthNumber(month).get) {
            monthMax = getMonthNumber(month).get
          }
        }
      }
      monthMax
    } catch {
      case e: NullPointerException => throw new NullPointerException("Utils class getMaxMonthNameItemized() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def getMonthsFromItemizedbill(spark: SparkSession, year: String): DataFrame = {
    try {

      spark.sqlContext.sql("select billing_month from mob_bill_summary_uz.itemized_bill where billing_year=" + year)
    } catch {
      case e: NullPointerException => throw new NullPointerException("Utils class getMonthFromItemizedbill() null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def getMonthNumber(month: String): Option[Integer] = {
    month match {
      case "January" => Some(1)
      case "February" => Some(2)
      case "March" => Some(3)
      case "April" => Some(4)
      case "May" => Some(5)
      case "June" => Some(6)
      case "July" => Some(7)
      case "August" => Some(8)
      case "September" => Some(9)
      case "October" => Some(10)
      case "November" => Some(11)
      case "December" => Some(12)
      case _ => None

    }
  }

  def getItemizedBillDataByMonth(spark: SparkSession, month: Integer, year: Integer): DataFrame = {
    try {
      spark.sqlContext.sql("select * from mob_bill_summary_uz.itemized_bill where billing_month='" + getMonthNumToString(month) + "' and billing_year=" + year)
    } catch {
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def getMonthNumToString(month: Int): String = {
    try {
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

  def getEmployeeData(spark: SparkSession): DataFrame = {
    try {
      spark.sqlContext.sql("select * from mob_bill_summary_uz.employee_master")
    } catch {
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def getAllItemizedData(spark: SparkSession):DataFrame = {
    try {
      spark.sqlContext.sql("select * from mob_bill_summary_uz.itemized_bill")
    } catch {
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def getMonthlyDatabyMonth(spark: SparkSession, month: Integer, year: Integer): DataFrame = {
    try {
      val MaxisMonthlyDF = spark.sqlContext.sql("select * from mob_bill_summary_uz.monthly_bill_summary where billing_month" + month + "and billing_year=" + year)
      return MaxisMonthlyDF
    } catch {
      case e: NullPointerException => throw new NullPointerException("maxis monthly bill story final null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def getAllMonthlyData(spark: SparkSession): DataFrame = {
    try {
      val MaxisMonthlyDF = spark.sqlContext.sql("select * from mob_bill_summary_uz.monthly_bill_summary")
      return MaxisMonthlyDF
    } catch {
      case e: NullPointerException => throw new NullPointerException("maxis monthly bill story final null data")
      case e: Exception => throw new Exception(e.getMessage)
    }
  }

  def addEmplyeeMonthlyTopTableToHive(dataframe: DataFrame): Unit = {
    try {
      dataframe.write.format("orc").mode(SaveMode.Append).saveAsTable("mob_bill_summary_uz" + "." + "employee_monthly_top_table")
      println("Table added")
    } catch {
      case nullException: NullPointerException => throw new NullPointerException("NullPointerException")
      case ex: Exception => println(ex.printStackTrace())
    }

  }

  def getMaxYearMonthly(spark: SparkSession): Integer = {
    import spark.implicits._
    try {
      var year = 0
      if (spark.sqlContext.sql("show tables in mob_bill_summary_uz like 'monthly_bill_summary'").collect().length == 1) {
        val yearDf = spark.sqlContext.sql("select max(billing_year) from mob_bill_summary_uz.monthly_bill_summary")
        val yearNum: Array[Integer] = yearDf.as[Integer].collect()
        year = yearNum(0)
      }
      year

    } catch {
      case e: NullPointerException => throw new NullPointerException("maxis monthly bill story null data")
      case e: Exception => throw new Exception(e.getMessage)
    }

  }

  def getMaxMonthMonthly(spark: SparkSession): Integer = {
    import spark.implicits._
    var maxMonthNumber = 0

    if (spark.sqlContext.sql("show tables in mob_bill_summary_uz like 'monthly_bill_summary'").collect().length == 1) {
      val yearDf = spark.sqlContext.sql("select max(billing_year) from mob_bill_summary_uz.monthly_bill_summary")
      val year: Array[Integer] = yearDf.as[Integer].collect()
      val months = getMonthsListMonthly(spark, year).as[String].collect()
      val monthsNumber = getMonthNumberMonthly(months)
      maxMonthNumber = monthsNumber.max
      val maxMonth = getMonthName(maxMonthNumber)

    }

    return maxMonthNumber
  }

  def getMonthName(name: Int): String = {

    name match {
      case 0 => "January"
      case 1 => "February"
      case 2 => "March"
      case 3 => "April"
      case 4 => "May"
      case 5 => "June"
      case 6 => "July"
      case 7 => "August"
      case 8 => "September"
      case 9 => "October"
      case 10 => "November"
      case 11 => "December"
      case _ => null

    }
  }

  def getMonthsListMonthly(spark: SparkSession, year: Array[Integer]): DataFrame = {
    spark.sqlContext.sql("select distinct billing_month from mob_bill_summary_uz.monthly_bill_summary where billing_year=" + year(0))
  }

  def getMonthNumberMonthly(months: Array[String]): List[Int] = {
    var list: ListBuffer[Int] = new ListBuffer[Int]()
    for (i <- 0 to months.length - 1) {
      val x: Option[Int] = months(i) match {
        case "January" => Some(1)
        case "February" => Some(2)
        case "March" => Some(3)
        case "April" => Some(4)
        case "May" => Some(5)
        case "June" => Some(6)
        case "July" => Some(7)
        case "August" => Some(8)
        case "September" => Some(9)
        case "October" => Some(10)
        case "November" => Some(11)
        case "December" => Some(12)
        case _ => None
      }
      list.append(x.get)
    }
    list.toList
  }
}
