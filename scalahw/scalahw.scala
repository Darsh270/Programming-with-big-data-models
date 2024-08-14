case class Neumaier(sum: Double, c: Double)

object HW {

  def q1(n: Int): List[Int] = {
    List.tabulate(n)(i => (i + 1) * (i + 1))
  }

  def q2(n: Int): Vector[Double] = {
    Vector.tabulate(n)(i => math.sqrt(i + 1))
  }

  def q3(x: Seq[Double]): Double = {
    if (x.isEmpty) 0.0 else x.reduce(_ + _)
  }

  def q4(x: Seq[Double]): Double = {
    if (x.isEmpty) 0.0 else x.reduce(_ * _)
  }

  def q5(x: Seq[Double]): Double = {
    x.foldLeft(0.0)((a, b) => a + math.log(b))
  }

  def q6(x: Seq[(Double, Double)]): (Double, Double) = {
    x.foldLeft((0.0, 0.0)) { case ((acc1, acc2), (first, second)) => (acc1 + first, acc2 + second) }
  }

  def q7(x: Seq[(Double, Double)]): (Double, Double) = {
    x.foldLeft((0.0, Double.NegativeInfinity)) { case ((sum, max), (first, second)) =>
      (sum + first, math.max(max, second))
    }
  }

  def q8(n: Int): (Int, Int) = {
    (1 to n).foldLeft((0, 1)) { case ((sum, product), i) => (sum + i, product * i) }
  }

  def q9(x: Seq[Int]): Int = {
    x.filter(_ % 2 == 0).map(i => i * i).foldLeft(0)(_ + _)
  }

  def q10(x: Seq[Double]): Double = {
    x.zipWithIndex.foldLeft(0.0) { case (acc, (value, index)) => acc + value * (index + 1) }
  }

  def q11(x: Seq[Double]): Double = {
    val result = x.foldLeft(Neumaier(0.0, 0.0)) { case (Neumaier(sum, c), item) =>
      val t = sum + item
      val cNew = if (math.abs(sum) >= math.abs(item)) c + ((sum - t) + item) else c + ((item - t) + sum)
      Neumaier(t, cNew)
    }
    result.sum + result.c
  }
  
}

