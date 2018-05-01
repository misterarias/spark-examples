package samples

import samples.calculators.PiCalculator
import samples.utils.SparkUtils

object Main {

  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.initContext

    val samples = 1000
    val pi = new PiCalculator(samples, sc)
    print(pi.montecarlo)

    sc.stop()
  }
}
