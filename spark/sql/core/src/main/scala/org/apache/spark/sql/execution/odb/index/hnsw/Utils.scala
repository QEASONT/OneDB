/*
 *  Copyright 2025 by ODB Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.sql.execution.odb.index.hnsw

import scala.reflect.ClassTag
import scala.util.Random

object Utils {
  val random = new Random()
  val numCores = Runtime.getRuntime.availableProcessors()

  def assert(cond: => Boolean, errorMsg: => String): Unit = {
    if (!cond) {
      throw new AssertionError(errorMsg)
    }
  }

  def randVec[T: ClassTag](dimension: Int)(rng: => T): Vec[T] = {
    Vec(Array.fill(dimension)(rng))
  }
}

