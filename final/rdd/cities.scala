import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ZipDistribution {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Zip Distribution")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs:///datasets/cities")
    val zipCounts = data.map { line =>
      val parts = line.split("\t")
      val zipCodes = parts(5).trim
      val numZipCodes = if (zipCodes.isEmpty) 0 else zipCodes.split(" ").length
      (numZipCodes, 1)
    }
    val result = zipCounts.reduceByKey(_ + _)
    result.saveAsTextFile("hdfs:///finalrdd")
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    sc.parallelize(List(
      "City1\tNY\tNew York\tCounty1\t12345\tZip1 Zip2\tID1",
      "City2\tNY\tNew York\tCounty2\t23456\tZip3\tID2",
      "City3\tCA\tCalifornia\tCounty3\t34567\tZip4 Zip5 Zip6\tID3",
      "City4\tCA\tCalifornia\tCounty4\t45678\tZip7\tID4"
    ))
  }

  def runTest(sc: SparkContext) = {
    val testRDD = getTestRDD(sc)
    val result = doCity(testRDD)
    result.collect().foreach(println)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("City Metrics Calculator").setMaster("local")
    val sc = new SparkContext(conf)
    runTest(sc) 
  }
}
