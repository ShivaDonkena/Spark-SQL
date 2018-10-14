
package com.core.hubd.spark.scala.core.homework

import java.io.File
import java.nio.file.Files

import com.core.hubd.spark.scala.sql.homework.Constants._
import com.core.hubd.spark.scala.sql.homework.MotelsHomeRecommendation
import com.core.hubd.spark.scala.sql.homework.MotelsHomeRecommendation._
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}


class MotelsHomeRecommendationTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfter
  with BeforeAndAfterAll with RDDComparisons {


  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val INPUT_EXCHANGE_RATE_SAMPLE = "src/test/resources/exchange_sample.txt"
  val INPUT_MOTELS_SAMPLE = "src/test/resources/motels_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.gz.parquet"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/expected_sql"


  private var outputFolder: File = _
  private var parquetFolder: File = _

  before {
    outputFolder = Files.createTempDirectory("output").toFile
    parquetFolder = Files.createTempDirectory("parquet").toFile
  }

  after {
    outputFolder.delete
    parquetFolder.delete
  }


  test("should read the bid data from the provided file as DataFrame") {
    createParquetFileFromCSV(INPUT_BIDS_SAMPLE, BIDS_HEADER)
    val expectedRDD = sc.parallelize(List(
      Row("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", null, "1.35",
        null, null, null, null, null, null, null, null, null, null),
      Row("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL", null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null)))

    val expected = spark.createDataFrame(expectedRDD, BIDS_HEADER)

    val result = MotelsHomeRecommendation.getRawBids(spark.sqlContext, parquetFolder.getCanonicalPath)
    assertDataFrameEquals(expected, result)
  }

  test("should collect the errors and return the result as DataFrame") {
    val inputRDD = sc.parallelize(List(
      Row("0000001", "17-17-17-2017", "ERROR_NO_BIDS_FOR_HOTEL", null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null),
      Row("0000001", "17-17-17-2017", "ERROR_NO_BIDS_FOR_HOTEL", null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null),
      Row("0000001", "17-17-17-2017", "ERROR_NO_BIDS_FOR_HOTEL", null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null),
      Row("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", null, "1.35",
        null, null, null, null, null, null, null, null, null, null),
      Row("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", null, "1.35",
        null, null, null, null, null, null, null, null, null, null),
      Row("0000001", "18-18-18-2018", "ERROR_NO_BIDS_FOR_HOTEL", null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null),
      Row("0000001", "18-18-18-2018", "ERROR_NO_BIDS_FOR_HOTEL", null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null)
    ))

    val inputDF = spark.createDataFrame(inputRDD, BIDS_HEADER)

    val expectedRDD = sc.parallelize(
      List(
        Row("18-18-18-2018", "ERROR_NO_BIDS_FOR_HOTEL", 2L),
        Row("17-17-17-2017", "ERROR_NO_BIDS_FOR_HOTEL", 3L)
      ))

    val struct = StructType(
      Array(
        StructField("BidDate", StringType),
        StructField("ERROR", StringType),
        StructField("ERROR_COUNTS", LongType, nullable = false)))
    val expected = spark.createDataFrame(expectedRDD, struct)

    val result = MotelsHomeRecommendation.getErroneousRecords(inputDF, spark.sqlContext)
    assertDataFrameEquals(expected, result)
  }

  test("should read the exchange rate information as DataFrame") {
    val expectedRDD = sc.parallelize(List(
      Row("11-06-05-2016", "Euro", "EUR", "0.803"),
      Row("11-05-08-2016", "Euro", "EUR", "0.873"),
      Row("10-06-11-2015", "Euro", "EUR", "0.987")))

    val expected = spark.createDataFrame(expectedRDD, EXCHANGE_RATES_HEADER)
    val result = MotelsHomeRecommendation.getExchangeRates(spark.sqlContext, INPUT_EXCHANGE_RATE_SAMPLE)
    assertDataFrameEquals(expected, result)
  }

  test("should convert date format") {
    val inputRDD = sc.parallelize(List(
      Row("11-06-05-2016", "Euro", "EUR", "0.803"),
      Row("11-05-08-2016", "Euro", "EUR", "0.873"),
      Row("10-06-11-2015", "Euro", "EUR", "0.987")))
    val inputDF = spark.createDataFrame(inputRDD, EXCHANGE_RATES_HEADER)

    import sqlContext.implicits._
    val expected = sc.parallelize(List(
      "2016-05-06 11:00",
      "2016-08-05 11:00",
      "2015-11-06 10:00")
    ).toDF("UDF(ValidFrom)")

    val result = inputDF.select(MotelsHomeRecommendation.getConvertDate($"ValidFrom"))
    assertDataFrameEquals(expected, result)
  }

  test("should join 2 DFs and convert the price to EUR and convert the data format") {
    val rawBidsRDD = sc.parallelize(List(
      Row("0000001", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", null, "1.35",
        null, null, null, null, null, null, null, null, null, null),
      Row("0000001", "15-04-08-2016", "ERROR_NO_BIDS_FOR_HOTEL", null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null),
      Row("0000002", "15-05-08-2016", "0.89", "0.92", "1.32", "2.07", "2.01", "1.35",
        "2.01", "2.01", "2.01", "2.01", "2.01", "2.01", "2.01", "2.01", "2.01", "2.01")
    ))
    val rawBidsDF = spark.createDataFrame(rawBidsRDD, BIDS_HEADER)

    val exchangeRatesRDD = sc.parallelize(
      List(
        Row("15-04-08-2016", "Euro", "EUR", "0.8111999234"),
        Row("15-05-08-2016", "Euro", "EUR", "0.7979321234")
      )
    )

    val exchangeRatesDF = spark.createDataFrame(exchangeRatesRDD, EXCHANGE_RATES_HEADER)
    val expectedRDD = sc.parallelize(
      List(
        Row("0000002", "2016-08-05 15:00", "US", 1.652),
        Row("0000002", "2016-08-05 15:00", "MX", 1.604),
        Row("0000002", "2016-08-05 15:00", "CA", 1.604),
        Row("0000001", "2016-08-04 15:00", "US", 1.679)
      )
    )
    val headers = StructType(Array("motelId", "BidDate")
      .map(field => StructField(field, StringType)))
      .add(StructField("losa", StringType, nullable = false))
      .add(StructField("finalPrice", DoubleType))
    val expected = spark.createDataFrame(expectedRDD, headers)
    val result = MotelsHomeRecommendation.getBids(rawBidsDF, exchangeRatesDF, sqlContext)
    assertDataFrameEquals(expected, result)
  }

  test("should load motels data from parquet") {
    import sqlContext.implicits._
    createParquetFileFromCSV(INPUT_MOTELS_SAMPLE, MOTELS_HEADER)

    val expected = sc.parallelize(Seq(
      ("0000001", "Olinda Windsor Inn"),
      ("0000002", "Merlin Por Motel"))
    ).toDF("motelId", "motelName")

    val result = MotelsHomeRecommendation.getMotels(spark.sqlContext, parquetFolder.getCanonicalPath)
    assertDataFrameEquals(expected, result)
  }

  test("should join the bids with motel names") {
    import sqlContext.implicits._
    val motels = sc.parallelize(Seq(
      ("0000001", "Olinda Windsor Inn"),
      ("0000002", "Merlin Por Motel"))
    ).toDF("motelId", "motelName")

    val bidsRDD = sc.parallelize(
      List(
        Row("0000002", "2016-08-05 15:00", "US", 10.0),
        Row("0000002", "2016-08-05 15:00", "CA", 12.0),
        Row("0000002", "2016-08-05 15:00", "CCCC", 12.0),
        Row("0000002", "2016-08-05 15:00", "MX", 11.0),
        Row("0000001", "2016-08-04 15:00", "US", 30.0)
      ))
    val headers = StructType(Array("motelId", "bidDate", "losa")
      .map(field => StructField(field, StringType)))
      .add(StructField("finalPrice", DoubleType))
    val bids = spark.createDataFrame(bidsRDD, headers)

    val expectedRDD = sc.parallelize(
      List(
        Row("0000002", "Merlin Por Motel", "2016-08-05 15:00", "CA", 12.0),
        Row("0000002", "Merlin Por Motel", "2016-08-05 15:00", "CCCC", 12.0),
        Row("0000001", "Olinda Windsor Inn", "2016-08-04 15:00", "US", 30.0)
      ))
    val expectedHeaders = StructType(Array("motelId", "motelName", "bidDate", "losa")
      .map(field => StructField(field, StringType)))
      .add(StructField("finalPrice", DoubleType))
    val expected = spark.createDataFrame(expectedRDD, expectedHeaders)

    val result = MotelsHomeRecommendation.getEnriched(sqlContext ,bids, motels)
    assertDataFrameEquals(expected, result)
  }

  test("should filter errors and create correct aggregates") {
    runIntegrationTest()
    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }


  private def runIntegrationTest(): Unit = {
    MotelsHomeRecommendation.processData(spark.sqlContext, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String): Unit = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }

  private def createParquetFileFromCSV(csvPath: String, struct: StructType): Unit = sqlContext
    .read.schema(struct).csv(csvPath)
    .write.mode(SaveMode.Overwrite).parquet(parquetFolder.getCanonicalPath)
}

