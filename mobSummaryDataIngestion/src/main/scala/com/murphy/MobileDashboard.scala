package com.murphy

import org.apache.spark.sql.SparkSession


object MobileDashboard {
  def main(args: Array[String]): Unit = {
    try {
      if (args.length == 0) {
        println("Give a file path")
        System.exit(0)
      }
      else if (args.length < 2) {
        println("Give a archive folder  path")
        System.exit(0)
      }
      else {
        val spark = SparkSession.builder().master("local").appName("mobiledashboard").enableHiveSupport().getOrCreate()
        val sourcePath = args(0)
        val destinationPath = args(1)
        val startingYearItemized = Utils.getMaxYearFromItemizedbill(spark)
        val startingMonthItemized = Utils.getMaxMonthFromItemizedbill(spark, startingYearItemized.toString)
        val startingYearMonthly = Utils.getMaxYearMonthly(spark)
        val startingMonthMonthly = Utils.getMaxMonthMonthly(spark)
        val itemizedObj = new ItemizedBIllStory(spark)
        val maxisMonthlyObj = new MaxisMonthlyBillStory(spark)
        val employeeMasterDataStory = new EmployeeMasterDataStory(spark)
        val celcomMonthlyBillStory = new CelcomMonthlyBillStory(spark)
        employeeMasterDataStory.addEmplyeeData(sourcePath, destinationPath)
        itemizedObj.ingestItemizedBill(sourcePath, destinationPath)
        maxisMonthlyObj.getMonthlyBill(sourcePath, destinationPath)
        celcomMonthlyBillStory.getMonthlyCelcomBill(sourcePath, destinationPath)
        val endYearItemized = Utils.getMaxYearFromItemizedbill(spark)
        val endMonthItemized = Utils.getMaxMonthFromItemizedbill(spark, startingYearItemized.toString)
        val endYearMonthly = Utils.getMaxYearMonthly(spark)
        val endMonthMonthly = Utils.getMaxMonthMonthly(spark)
        val employeeMonthlyTopTable = new EmployeeMonthlyTopTable(spark)
        val employeeIemizedBillStory = new EmplyeeItemizedBillStory(spark)
        employeeIemizedBillStory.createEmployeeItemized(startingYearItemized, startingMonthItemized, endYearItemized, endMonthItemized)
        employeeMonthlyTopTable.ingestEmployeeMonthlyTopTable(startingYearMonthly, startingMonthMonthly, endYearMonthly, endMonthMonthly)
      }

    } catch {
      case e: Exception => println(e)
    }
  }
}
