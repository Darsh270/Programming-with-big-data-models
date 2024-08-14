
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q1 {
  def doCity(input: RDD[String]): RDD[(String, (Int, Int, Int))] = {
    val cleanData = input.map(line => line.split("\t"))
                        .filter(line => line.length >= 6 && line(4).forall(_.isDigit))

    // Update indices based on new data format
    val stateMetrics = cleanData.map(line => (line(1), (1, line(4).toInt, line(5).split(" ").length)))
                                .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, Math.max(a._3, b._3)))

    stateMetrics
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
    runTest(sc)  // Ensure that the testing function is actually called within main
  }
}

