import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q2 {
  
  def doOrders(customers: RDD[String]): RDD[(String, Int)] = {
    val header = customers.first()
    val data = customers.filter(_ != header)
    
    val countryCounts = data.map { line =>
      val parts = line.split("\t")
      (parts(1), 1)
    }
    val counts = countryCounts.reduceByKey(_ + _)
    counts
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    sc.parallelize(List(
      "CustomerID\tCountry",
      "15005\tGermany",
      "13019\tUSA",
      "12980\tAustralia",
      "14577\tIndia",
      "13298\tCanada",
      "15005\tGermany"
    ))
  }

  def runTest(sc: SparkContext) = {
    val testRDD = getTestRDD(sc)
    val result = doOrders(testRDD)
    result.collect().foreach(println)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Customer Country Counter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    runTest(sc)
  }
}

