package samples.calculators

import org.scalatest._
import samples.utils.TestHelper

class SparkLibrarySpec extends TestHelper with Matchers  {

  "A PiCalculator" should "not aproximate PI after 1 iteration" in {
    val calc = new PiCalculator(1, sc)
    calc.montecarlo should not be 3.14
  }
}