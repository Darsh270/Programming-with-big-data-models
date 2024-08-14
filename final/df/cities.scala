import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ZipDistributionDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
                            .appName("Zip Distribution DataFrame")
                            .getOrCreate()
    import spark.implicits._

    val data = spark.read.option("sep", "\t").csv("hdfs:///datasets/cities")
    val result = data.map(row => {
      val zipCodes = row.getString(5).trim
      val numZipCodes = if (zipCodes.isEmpty) 0 else zipCodes.split(" ").length
      (numZipCodes, 1)
    }).toDF("ZipCodeCount", "CityCount")
      .groupBy("ZipCodeCount")
      .sum("CityCount")
      .withColumnRenamed("sum(CityCount)", "CityCount")

    result.write.format("csv").option("header", "false").save("hdfs:///finaldf")
  }

  def registerZipCounter(spark: SparkSession) = {
    val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
    spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
  }

  def doCity(input: DataFrame): DataFrame = {
    input// Drop rows with null values
      .filter(!col("State Abbreviation").startsWith("State Abbreviation"))
      .groupBy(col("State Abbreviation"))
      .agg(
        count("*").alias("num_cities"),
        sum(col("Population")).alias("total_population"),
        max(col("zip_count")).alias("max_zip_count")
      )
      .orderBy(col("State Abbreviation"))
  }

  def getDF(spark: SparkSession): DataFrame = {
    val schema = StructType(Seq(
      StructField("City", StringType),
      StructField("State Abbreviation", StringType),
      StructField("State", StringType),
      StructField("County", StringType),
      StructField("Population", LongType),
      StructField("Zip Codes (space separated)", StringType),
      StructField("ID", StringType)
    ))

    val initialDF=spark.read
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema)
      .csv("/datasets/cities/cities.csv")
      .na.fill(0, Seq("Population"))  // Replace null values in Population column with 0
      .na.fill("0", Seq("Zip Codes (space separated)"))  // Replace null values in Zip Codes column with "0"
      .withColumn("zip_count", expr("zipCounter(`Zip Codes (space separated)`)"))

    val populationFixedDF = initialDF.withColumn("Population", coalesce(col("Population"), lit(0)))

    val zipFixedDF = populationFixedDF.withColumn("Zip Codes (space separated)", coalesce(col("Zip Codes (space separated)"), lit("0")))

    val finalDF = zipFixedDF.filter(col("City").isNotNull && col("State Abbreviation").isNotNull)

    finalDF.withColumn("zip_count", expr("zipCounter(`Zip Codes (space separated)`)"))
  }



  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("YourApp")
      .master("local") // Or any other master URL
      .getOrCreate()
    registerZipCounter(spark) // tells the spark session about the UDF
    spark
  }

  def getTestDF(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val testData = Seq(
      ("City1", "WA", "Washington", "CountyA", 1000L, "98001 98002 98003", "ID1"),
      ("City2", "WA", "Washington", "CountyB", 2000L, "98101 98102", "ID2"),
      ("City3", "NY", "New York", "CountyC", 1500L, "10001 10002", "ID3"),
      ("City4", "NY", "New York", "CountyD", 3000L, "10003 10004 10005", "ID4"),
      ("City5", "CA", "California", "CountyE", 2500L, "90001", "ID5"),
      ("City6", "CA", "California", "CountyF", 1800L, "90002 90003", "ID6")
    )

    val testDF = testData.toDF("City", "State Abbreviation", "State", "County", "Population", "Zip Codes (space separated)", "ID")

    val resultDF = testDF.withColumn("zip_count", expr("zipCounter(`Zip Codes (space separated)`)"))

    resultDF
  }


  def runTest(spark: SparkSession) = {
    val testDF = getTestDF(spark)
    val resultDF = doCity(testDF)
    resultDF.show()
  }
  def saveit(counts: DataFrame, name: String) = {
    counts.write.format("csv").mode("overwrite").save(name)

  }
  import org.apache.spark.sql.DataFrame

  def printRowsWithNull(df: DataFrame): Unit = {
    val rows: Array[Row] = df.collect()
    rows.foreach { row =>
      val hasNull = row.toSeq.exists {
        case null => true
        case _    => false
      }
      if (hasNull) {
        println("Row with null values:")
        println(row.mkString(", "))
      }
    }
  }
  def mygetTestDF(spark: SparkSession): DataFrame = {
    val schema = StructType(Seq(
      StructField("City", StringType),
      StructField("State Abbreviation", StringType),
      StructField("State", StringType),
      StructField("County", StringType),
      StructField("Population", LongType),
      StructField("Zip Codes (space separated)", StringType),
      StructField("ID", StringType)
    ))

    // Read the CSV file with the specified schema, skipping the header
    val initialDF = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema)
      .csv("C:\\Users\\sanglap\\shivam spark assignment\\untitled\\testing\\test.csv")
      .dropDuplicates() // Remove any duplicate rows

    val populationFixedDF = initialDF.withColumn("Population", coalesce(col("Population"), lit(0)))

    val zipFixedDF = populationFixedDF.withColumn("Zip Codes (space separated)", coalesce(col("Zip Codes (space separated)"), lit("0")))

    val finalDF = zipFixedDF.filter(col("City").isNotNull && col("State Abbreviation").isNotNull)

    val resultDF = finalDF.withColumn("zip_count", expr("zipCounter(`Zip Codes (space separated)`)"))

    val filteredDF = resultDF.filter(!col("State Abbreviation").startsWith("State Abbreviation"))

    filteredDF
  }


}
