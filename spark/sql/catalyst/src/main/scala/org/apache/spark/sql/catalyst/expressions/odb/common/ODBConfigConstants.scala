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

package org.apache.spark.sql.catalyst.expressions.odb.common

object ODBConfigConstants {

  val DEBUG: Boolean = false
  var GLOBAL_INDEXED_PIVOT_COUNT = 600 // parameter 1
  var PIVOT_INDEX: Int = 20000 // useless
  val NUM_HNSW_PARTITIONS = 100 // useless
  val RPCHost = "127.0.0.1"
  val RPCPort = 50051
  val N = 9
  val ODB_KNN_ESTIMATED_RATE_DEFAULT = 1


  // rtree
  var SAMPLE_SIZE: Long = 20000 // parameter 2

  var RTREE_GLOBAL_MAX_ENTRIES_PER_NODE = 100 // parameter 4
  var RTREE_LOCAL_MAX_ENTRIES_PER_NODE = 100 // parameter 5

  var RTREE_GLOBAL_NUM_PARTITIONS = 20 // parameter 6
  var RTREE_LOCAL_NUM_PARTITIONS = 20 // parameter 7
  val MAX_ENTRIES_PER_NODE = 10

  // BPlusTree
  var BPlusTreeOrder = 30
  var GlobalBPlusTreeOrder = 30

  val SMALL_SET_LIMIT = 8192
  val LARGE_SET_LIMIT = 16384
  val UP_LIMIT: Int = SMALL_SET_LIMIT

  // M-Tree
  var M_TREE_LEAF_ORDER = 300
  var M_TREE_INNER_ORDER = 8
  var LOCAL_M_TREE_LEAF_ORDER = 30
  var LOCAL_M_TREE_INNER_ORDER = 3
  var ODB_MODE = 3
  var DATA_TYPE = 0

  // knn
  var KNN = 8
  var RANGE = 300d

  // Distance function 0: Euclidean, 1: L1, 2: word cosine

  val DISTANCE_FUNCTION_PATH = "hdfs://10.214.151.180:8020/CustomDistanceFunction.txt"

}
