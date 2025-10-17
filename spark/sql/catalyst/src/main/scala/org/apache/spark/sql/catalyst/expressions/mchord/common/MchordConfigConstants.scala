/*
 *  Copyright 2023 by DIMS Project
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

package org.apache.spark.sql.catalyst.expressions.mchord.common


object MchordConfigConstants {
  // basic
  val THRESHOLD_LIMIT = 100.0
  val SAMPLE_SIZE: Long = 1L * 3 * 1024 * 1024

  // global
  var GLOBAL_INDEXED_PIVOT_COUNT: Int = 200


  // load balancing
  val BALANCING_MIN_SAMPLE_SIZE = 1000

  val iDistanceConstant = 100000

  // Mchord

  // knn
  var KNN = 8
  var RANGE = 100d

  // BPlusTree
  val BPlusTreeOrder = 100
  var DATA_TYPE = 0

  // Distance function 0: Euclidean, 1: L1, 2: word cosine
  var MCHORD_DISTANCE_TYPE = 1
}
