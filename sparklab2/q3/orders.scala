import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q3 {

  def doOrders(customers: RDD[String], orders: RDD[String]): RDD[(String, Int)] = {
    val customersHeader = customers.first()
    val ordersHeader = orders.first()

    val customersData = customers.filter(_ != customersHeader)
    val ordersData = orders.filter(_ != ordersHeader)

    val customerCountryMap = customersData.map { line =>
      val parts = line.split("\t", -1)
      (parts(0), parts(1))
    }

    val orderQuantityMap = ordersData.map { line =>
      val parts = line.split("\t", -1)
      (parts(4), parts(2).toInt)
    }

    val countryQuantity = orderQuantityMap
      .join(customerCountryMap)  
      .map { case (_, (quantity, country)) => (country, quantity) }
      .reduceByKey(_ + _)        

    countryQuantity
  }

  def getTestRDD(sc: SparkContext): (RDD[String], RDD[String]) = {
    val customers = List(
      "CustomerID\tCountry",
      "12346\tUnited Kingdom",
      "12347\tIceland",
      "12348\tFinland",
      "12349\tItaly",
      "12350\tNorway",
      "12352\tNorway",
      "12353\tBahrain",
      "12354\tSpain",
      "12355\tBahrain",
      "12356\tPortugal",
      "12357\tSwitzerland",
      "12358\tAustria",
      "12359\tCyprus",
      "12360\tAustria",
      "12361\tBelgium",
      "12362\tBelgium"
    )
    val orders = List(
      "InvoiceNo\tStockCode\tQuantity\tInvoiceDate\tCustomerID",
      "536365\t85123A\t6\t12/1/2010 8:26\t17850",
      "536365\t71053\t6\t12/1/2010 8:26\t17850",
      "536367\t84879\t32\t12/1/2010 8:34\t13047",
      "536367\t21754\t3\t12/1/2010 8:34\t13047"
    )

    val customersRDD = sc.parallelize(customers)
    val ordersRDD = sc.parallelize(orders)
    (customersRDD, ordersRDD)
  }

  def runTest(sc: SparkContext) = {
    val (testCustomersRDD, testOrdersRDD) = getTestRDD(sc)
    val result = doOrders(testCustomersRDD, testOrdersRDD)
    result.collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Total Quantity By Country").setMaster("local[*]")
    val sc = new SparkContext(conf)

    runTest(sc) 
  }
}

