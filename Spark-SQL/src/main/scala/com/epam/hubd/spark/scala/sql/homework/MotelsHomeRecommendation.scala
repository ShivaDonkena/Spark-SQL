package com.core.hubd.spark.scala.sql.homework

import com.core.hubd.spark.scala.sql.homework.Constants._
import com.core.spark.sql.domain.BidItem
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory


object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
     val logger = Logger(LoggerFactory.getLogger(MotelsHomeRecommendation.getClass()))
    logger.info("Main method started!!")
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")
    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sqlSparkSession: SparkSession = SparkSession
      .builder()
      .appName("MaxBidSearch")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    processData(sqlSparkSession.sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)
    sqlSparkSession.stop()
     logger.info("End of the main method!!")
  }

  def processData(sqlContext: SQLContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String): Unit = {
    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath).cache()

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids, sqlContext).coalesce(1)

    erroneousRecords.write
      .format(CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = getConvertDate

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates, sqlContext)


    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(sqlContext, bids, motels).coalesce(1)
    enriched.write
      .format(CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  /**
    * Read parquet file with defined schema as a DataFrame
    *
    * @param sqlContext
    * @param bidsPath path to a source file
    * @return DataFrame of bids
    */
  def getRawBids(sqlContext: SQLContext, bidsPath: String): DataFrame =
    sqlContext.read.schema(BIDS_HEADER).parquet(bidsPath)

  /**
    * We filter source DataFrame rows with match on ERROR and group them by bidDate
    * and error message and and count amount of messages in each group.
    * Also we save it as output result file.
    *
    * @param rawBids source DataFrame
    * @return DataFrame with erroneous rows
    */
  def getErroneousRecords(rawBids: DataFrame, spark: SQLContext): DataFrame = {
    import rawBids.sqlContext.implicits._

    val errorRecords = rawBids.filter(bids => bids.getString(Constants.BIDS_HEADER.fieldIndex("HU")).contains("ERROR"))
    val groupedRecords = errorRecords.select($"motelId", $"bidDate", $"HU").groupBy($"bidDate", $"HU").count().alias("Count");
    groupedRecords.select($"bidDate", $"HU", $"Count")
  }

  /**
    * Reading the exchange rates.
    *
    * @param spark             SparkSession
    * @param exchangeRatesPath path to source file
    * @return DataFrame of exchange rates
    */
  def getExchangeRates(spark: SQLContext, exchangeRatesPath: String): DataFrame =
    spark.read.schema(EXCHANGE_RATES_HEADER).csv(exchangeRatesPath)

  /**
    * @return Date format conversion UDF
    *         [HH-dd-MM-yyyy] --> ["yyyy-MM-dd HH:mm"]
    */
  def getConvertDate: UserDefinedFunction =
    udf((date: String) => OUTPUT_DATE_FORMAT.print(INPUT_DATE_FORMAT.parseDateTime(date)))

  /*
   * To multiply the price and exchangeRate with each other.
   */
  def priceMultiWithExchangeRate: UserDefinedFunction =
    udf((price: Double, exchangeRate: Double) => price * exchangeRate)

  /**
    * Removing the error records
    * Joining the bidsItem and the exchangeRates tables.
    * group the  bids for each and every losa and convert price from USD to EUR based on date specified.
    * Rounding the  price value  to 3 decimal digits.
    * Converting the date format from [HH-dd-MM-yyyy] --> ["yyyy-MM-dd HH:mm"].
    *
    * @param rawBids       DataFrame of bids
    * @param exchangeRates DataFrame of echange rates
    * @return DataFrame with full price for each location
    */
  def getBids(rawBids: DataFrame, exchangeRates: DataFrame, spark: SQLContext): DataFrame = {
    import rawBids.sqlContext.implicits._


    //Removing the records with ERROR at HU and  has empyty values for US, MX, CA .
    val bidsDF = rawBids.filter(!$"HU".contains("ERROR"))

    //Creating the bidsItems for the US,MX,CA seperately.
    val bidsItem = bidsDF.flatMap(seq => Array(BidItem(seq.getString(Constants.BIDS_HEADER.fieldIndex("MotelID")), seq.getString(Constants.BIDS_HEADER.fieldIndex("BidDate")), "US", seq.getString(Constants.BIDS_HEADER.fieldIndex("US"))),
      BidItem(seq.getString(Constants.BIDS_HEADER.fieldIndex("MotelID")), seq.getString(Constants.BIDS_HEADER.fieldIndex("BidDate")), "MX", seq.getString(Constants.BIDS_HEADER.fieldIndex("MX"))),
      BidItem(seq.getString(Constants.BIDS_HEADER.fieldIndex("MotelID")), seq.getString(Constants.BIDS_HEADER.fieldIndex("BidDate")), "CA", seq.getString(Constants.BIDS_HEADER.fieldIndex("CA"))))).toDF()


    //Joining the bidsItem table with the exchange table.
    bidsItem.join(exchangeRates, bidsItem.col("BidDate") === exchangeRates.col("ValidFrom"))

      // Select motelId, bidDate,losa ,price , ExchangeRate and then filter convertion of price and ExchangeRate to double.
      .select($"motelId", $"bidDate", $"losa", $"price", $"ExchangeRate").filter(checkTheExchangeAndPrice _)

      //Adding the finalprice column by multiplying the price, ExchangeRate and rounding it to 3 decimal digits.
      .withColumn("finalPrice", round(priceMultiWithExchangeRate($"price", $"ExchangeRate"), 3))
      .select($"motelId", getConvertDate($"bidDate").as("BidDate"), $"losa", $"finalPrice")


  }


  /*
    To check the both the price value and the exchange values after converting to double.
   */
  private def checkTheExchangeAndPrice(rowValues: Row) = {
    try {
      !rowValues.getAs[String]("ExchangeRate").toDouble.isNaN && !rowValues.getAs[String]("price").toDouble.isNaN
    } catch {
      case _: NumberFormatException => false
      case _: NullPointerException => false
    }
  }

  /**
    * Reading the  parquet file with defined schema as a DataFrame
    *
    * @param sqlContext SQlContext
    * @param motelsPath path to source file
    * @return DataFrame of motels
    */
  def getMotels(sqlContext: SQLContext, motelsPath: String): DataFrame = sqlContext.read.parquet(motelsPath)

  /**
    * Finding the Top price data of the record motelId ,motelName,bidDate,losa,finalPrice using the rank.
    *
    * @param bids   DataFrame of bids'
    * @param motels DataFrame of motels
    * @return DataFrame of motels with name and highest price
    */
  def getEnriched(sqlContext: SQLContext, bids: DataFrame, motels: DataFrame): DataFrame = {

    //Creating the motels_table table form motels
    motels.createOrReplaceTempView("motels_table")

    //get the getHighestBids by creating the extra column rank for finding the higest price.
    val getHighestBids = bids.withColumn("rank", rank.over(Window.partitionBy("motelId", "bidDate").orderBy(desc("finalPrice"))))

    //Creating the enriched_table.
    getHighestBids.createOrReplaceTempView("enriched_table")

    //Read the motelId ,motelName,bidDate,losa,finalPrice by joining the motels_table table with the enriched_table based on the common column motelID
    sqlContext.sql("select enrich.motelId, motel.motelName, enrich.bidDate, enrich.losa, finalPrice  from enriched_table enrich ,motels_table motel where enrich.rank==1 and enrich.motelId==motel.motelId");

  }
}
