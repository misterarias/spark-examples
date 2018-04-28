package samples.calculators

import org.apache.spark.SparkContext

/*
 * Copyright 2018
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
