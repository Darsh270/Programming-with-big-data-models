import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q3 {
  def doRetail(records: RDD[String]): RDD[(String, Double)] = {
    val filteredRecords = records.filter(line => !line.startsWith("InvoiceNo"))
    val orderCosts = filteredRecords.map(line => {
      val fields = line.split("\\t")
      val invoiceNo = fields(0)
      val quantity = fields(3).toDouble
      val unitPrice = fields(5).toDouble
      val totalCost = quantity * unitPrice
      (invoiceNo, totalCost)
    })
    val orderTotalCosts = orderCosts.reduceByKey(_ + _)
    orderTotalCosts
  }

  def runTest(sc: SparkContext): Unit = {
    val testRDD = getTestRDD(sc)
    val testResult = doRetail(testRDD)
    println("Test Results:")
    testResult.collect().foreach(println)
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    val myLines = List(
      "536365\\t85123A\\tWHITE HANGING HEART T-LIGHT HOLDER\\t6\\t12/1/10 8:26\\t2.55\\t17850\\tUnited Kingdom",
      "536365\\t71053\\tWHITE METAL LANTERN\\t6\\t12/1/10 8:26\\t3.39\\t17850\\tUnited Kingdom",
      "536365\\t84406B\\tCREAM CUPID HEARTS COAT HANGER\\t-8\\t12/1/10 8:26\\t2.75\\t17850\\tUnited Kingdom"
    )
    sc.parallelize(myLines, 3)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RetailAnalysis")
    val sc = new SparkContext(conf)
    try {
      val records = sc.textFile("/datasets/retailtab")
      val orderTotalCosts = doRetail(records)
      orderTotalCosts.saveAsTextFile("hdfs:///user/dsw5439/spark3output")
    } finally {
      sc.stop()
    }
  }
}
