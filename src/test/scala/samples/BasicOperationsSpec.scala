package samples

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Matchers
import utils.TestHelper

class BasicOperationsSpec extends TestHelper with Matchers {
  "A fold" should "return the mix of a function and an initial value " in {
    var rdd = sc.parallelize(1 to 4)
    val fun = (op1: Int, op2: Int) => op1 + op2

    var zero_value = 0 // If you change this, beware the partition number!
    rdd.fold(zero_value)(fun) should be(10)

    // Let's modify the configuration
    sc.stop()
    val sparkConf: SparkConf = sc.getConf
    sparkConf.setMaster("local[1]") // mono core
    sc = SparkContext
      .getOrCreate(sparkConf)

    rdd = sc.parallelize(1 to 4)
    zero_value = 1
    rdd.fold(zero_value)(fun) should be(12)
  }

  "A reduce" should "be a commutative operation" in {
    val rdd = sc.parallelize('a' to 'f')
    val fun1 = (op1: Char) => s"$op1$op1"
    val fun2 = (op1: String, op2: String) => s"$op1$op2"

    rdd.map(fun1).reduce(fun2) should not be "aabbcceeff"
  }
}
