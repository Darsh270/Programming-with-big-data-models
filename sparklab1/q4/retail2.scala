import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q4 {
  def doRetail(records: RDD[String]): RDD[(String, (Int, Double))] = {
    val filteredRecords = records.filter(line => !line.startsWith("InvoiceNo"))

    val orderDetails = filteredRecords.map(line => {
      val fields = line.split("\\t")
      val invoiceNo = fields(0)
      val quantity = fields(3).toInt
      val unitPrice = fields(5).toDouble
      val totalCost = quantity * unitPrice
      (invoiceNo, (quantity, totalCost))
    })

    val orderTotals = orderDetails.reduceByKey { (a, b) =>
      val (qty1, cost1) = a
      val (qty2, cost2) = b
      val totalQty = qty1 + qty2
      val positiveQty = math.max(totalQty, 0)
      val negativeQty = math.max(-totalQty, 0)
      val adjustedQty = positiveQty - negativeQty
      val totalCost = positiveQty * cost1 / qty1 + negativeQty * cost2 / qty2
      (adjustedQty, totalCost)
    }

    orderTotals
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
      "536365\\t84406B\\tCREAM CUPID HEARTS COAT HANGER\\t-8\\t12/1/10 8:26\\t2.75\\t17850\\t
