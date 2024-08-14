

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Q4 {
  
  def doOrders(customers: RDD[String], orders: RDD[String], items: RDD[String]): RDD[(String, Double)] = {
    val customerMap = customers
      .map(_.split("\t"))
      .filter(parts => parts.length == 2 && parts(0).trim.nonEmpty)
      .map(parts => (parts(0), parts(1)))  // (CustomerID, Country)

    val ordersMap = orders
      .map(_.split("\t"))
      .filter(parts => parts.length >= 5)
      .map(parts => (parts(4), (parts(1), parts(2).toInt)))  // (CustomerID, (StockCode, Quantity))

    val itemPriceMap = items
      .map(_.split("\t"))
      .filter(parts => parts.length == 3)
      .map(parts => (parts(0), parts(2).toDouble))  // (StockCode, UnitPrice)

    val ordersWithPrices = ordersMap
      .join(itemPriceMap)
      .map {
        case (customerId, ((stockCode, quantity), unitPrice)) => (customerId, quantity * unitPrice)
      }

    val totalSpentByCountry = customerMap
      .join(ordersWithPrices)
      .map {
        case (customerId, (country, totalPrice)) => (country, totalPrice)
      }
      .reduceByKey(_ + _)

    totalSpentByCountry
  }

  def getTestRDD(sc: SparkContext): (RDD[String], RDD[String], RDD[String]) = {
    val customersTestData = sc.parallelize(Seq(
      "12346\tUnited Kingdom",
      "12347\tIceland",
      "12348\tFinland"
    ))
    
    val ordersTestData = sc.parallelize(Seq(
      "536365\t85123A\t6\t12/1/2010 8:26\t12346",
      "536365\t71053\t6\t12/1/2010 8:26\t12346",
      "536365\t22752\t2\t12/1/2010 8:26\t12347"
    ))
    
    val itemsTestData = sc.parallelize(Seq(
      "85123A\tINFLATABLE POLITICAL GLOBE\t1.66",
      "71053\tGROOVY CACTUS INFLATABLE\t0.85",
      "22752\tSET 7 BABUSHKA NESTING BOXES\t2.95"
    ))
    
    (customersTestData, ordersTestData, itemsTestData)
  }

  def runTest(sc: SparkContext) = {
    val (customers, orders, items) = getTestRDD(sc)
    val result = doOrders(customers, orders, items)
    result.collect().foreach(println)
  }
}

