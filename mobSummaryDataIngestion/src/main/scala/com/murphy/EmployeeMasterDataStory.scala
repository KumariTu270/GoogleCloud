package com.murphy

import java.io._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, types}

class EmployeeMasterDataStory(val spark: SparkSession) {

  def addEmplyeeData(inPath: String, destinationPath: String): Unit = {
    try {
      val folderName: String = "Employee_data/"
      val fileList: List[String] = Utils.getFileList(inPath + folderName)
      if (fileList.isEmpty) {
        println("no employee master files present in folder")
      }
      else for (i <- fileList.indices) {
        val fileObject = fileList(i)
        val df_new = spark.sqlContext.read
          .format("com.crealytics.spark.excel")
          .option("sheetName", "Master 18")
          .option("useHeader", "true")
          .option("treatEmptyValuesAsNulls", "false")
          .option("inferSchema", "false")
          .option("startColumn", 0)
          .option("endColumn", 20)
          .option("skipFirstRows", 3)
          .load(fileObject)
        import spark.implicits._
        val finalDf = df_new.withColumnRenamed("H/P NO.", "hp_number12")
          .withColumnRenamed("User / Custodian (As per HR list)", "user_custodian")
        val df_filtered = finalDf.filter($"hp_number12".notEqual("null") && $"hp_number12".notEqual("NEW"))

        val showData1 = finalDf.withColumn("STD", Utils.extract_std_code($"hp_number12"))
          .select(col("STD").alias("STD_code").cast(types.StringType),
            col("USER").alias("username").cast(types.StringType),
           // Utils.extract_phone_number(col("hp_number12")).alias("contact_number").cast(types.StringType),
            col("Phone Model").alias("phone_model").cast(types.StringType),
            col("Refresh").alias("refresh").cast(types.StringType),
            col("Location").alias("location").cast(types.StringType),
            col("Deliver Date").alias("deliver_date").cast(types.StringType),
            col("Provider").alias("provider").cast(types.StringType),
            col("Type").alias("type").cast(types.StringType),
            col("Custodian").alias("custodian").cast(types.StringType),
            col("user_custodian"),
            col("Cost Center").alias("cost_center").cast(types.StringType),
            col("Department").alias("department").cast(types.StringType),
            col("Department HR ").alias("department_HR").cast(types.StringType),
            col("Title").alias("title").cast(types.StringType),
            col("Supervisor").alias("supervisor").cast(types.StringType),
            col("Head Of Department").alias("head_of_department").cast(types.StringType))
        //showData1.show()
        Utils.addEmplyeeDataToHive(showData1)
        Utils.moveFileToArchive(fileList(i), destinationPath)
      }
    } catch {
      case e: FileNotFoundException => throw new FileNotFoundException("Employee Master bill file not found")
    }
  }

}
