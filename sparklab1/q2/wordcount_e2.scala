

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q2 {

  def doWordCount(lines: RDD[String]): RDD[(String, Int)] = {
    lines.flatMap(_.split("\\s+"))
         .filter(_.contains('e'))
         .map(word => (word, 1))
         .reduceByKey(_ + _)
         .filter(_._2 >= 2) 
  }

  def runTest(sc: SparkContext): Unit = {
    val testRDD = getTestRDD(sc)
    val testResult = doWordCount(testRDD)
    testResult.collect().foreach(println)
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    val mylines = List("Here are some examples", "Each sentence is split", "Eerie sounds were heard")
    sc.parallelize(mylines, 3)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountWithE2").setMaster("local[*]") 
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/datasets/wap")
    val wordCounts = doWordCount(lines)
    saveit(wordCounts, "hdfs:///user/dsw5439/spark1output")
  }

  def saveit(counts: RDD[(String, Int)], name: String): Unit = {
    counts.saveAsTextFile(name)
  }
}

