package com.core.hubd.spark.scala.sql.homework

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.format.DateTimeFormat

object Constants {

  val DELIMITER = ","

  val CSV_FORMAT = "com.databricks.spark.csv"

  val BIDS_HEADER = StructType(
    Array("MotelID", "BidDate", "HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR", "BE", "I", "JP", "IN", "HN", "GY", "DE")
      .map(field => StructField(field, StringType)))

  val EXCHANGE_RATES_HEADER = StructType(Array("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate").map(field => StructField(field, StringType, true)))

  val MOTELS_HEADER = StructType(
    Array("motelId", "motelName")
      .map(field => StructField(field, StringType)))

  val TARGET_LOSAS = Seq("US", "CA", "MX")

  val INPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
}
