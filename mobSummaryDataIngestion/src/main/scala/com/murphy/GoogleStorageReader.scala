package com.murphy

import org.apache.spark.sql.SparkSession
object GoogleStorageReader {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("GCS test")
      .master("local")
      .config("spark.sql.warehouse.dir", "E:\\mobSummaryDataIngestion\\spark-warehouse")
      .getOrCreate()
    val jsonKeyFile = "E:\\ConnsKPI scripts\\gcstest\\gcstest\\google_key.json"
    spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile", jsonKeyFile)
     val df = spark.sqlContext.read
       .format("com.crealytics.spark.excel")
       .option("sheetName", "Master 18")
       .option("useHeader", "true")
       .option("treatEmptyValuesAsNulls", "false")
       .option("inferSchema", "false")
       .option("startColumn", 0)
       .option("endColumn", 20)
       .option("skipFirstRows", 3)
      .load("gs://bucket-for-practice/MAXIS_CELCOM_IT_Masterlist-2018.xlsx"
    )
    df.show()
  }
}
