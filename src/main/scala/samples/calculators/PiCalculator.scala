package samples.calculators

import org.apache.spark.SparkContext

class PiCalculator(numSamples: Int, sc: SparkContext) {

  def montecarlo: Double = {
    val inSamples = sc.parallelize(1 to numSamples).filter(_ => {
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }).count()

    4.0 * inSamples / numSamples
  }
}
