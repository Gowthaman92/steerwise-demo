package com.demo.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.functions.unix_timestamp

object Main {

  def main(args: Array[String]) {
  
    val spark = getSparkSession()
    val filename = args(0)
    val csvDf = spark.read.option("header","true").csv(filename)
    
    val transformedDf = transformDF(csvDf,spark)
    
    transformedDf.write.format("org.elasticsearch.spark.sql").option("es.nodes.wan.only","true").option("es.nodes", "localhost").mode("Overwrite").save("sales/spark")
  
    spark.stop()
  }

  def getSparkSession(): SparkSession = {

    val conf: SparkConf = new SparkConf().setAppName("DataLoad").set("spark.ui.enabled", "true")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
    return spark
  }

  def calculateAmount(quantity: Int, price: Float): Float = {

    return quantity * price;

  }

  def transformDF(df: DataFrame, spark: SparkSession) : DataFrame = {

    import spark.implicits._

    val calculateAmountUDF = spark.udf.register("calculateAmount", (quantity: Int, price: Float) => quantity * price)
    val datatypeChangedDF = df.selectExpr("cast (InvoiceNo as int) as invoice_no", "cast (CustomerID as int) as customer_id", "StockCode as stock_code", "Description as description", "InvoiceDate as invoice_date", "cast (Quantity as int) as quantity", "cast(UnitPrice as float) as unit_price", "Country as country")
    val timeStamp = unix_timestamp($"invoice_date", "dd/MM/yy HH:mm")
    val timeStamFormattedDf = datatypeChangedDF.withColumn("invoice_date_tmp", from_unixtime(timeStamp,"yyyy-MM-dd HH:mm")).drop("invoice_date").withColumnRenamed("invoice_date_tmp", "invoice_date")

    var amountAddedDf = timeStamFormattedDf.withColumn("amount", calculateAmountUDF($"quantity", $"unit_price"))
    amountAddedDf = amountAddedDf.withColumn("amount_temp", round($"amount", 3)).drop("amount").withColumnRenamed("amount_temp", "amount")
    amountAddedDf
  }

}
