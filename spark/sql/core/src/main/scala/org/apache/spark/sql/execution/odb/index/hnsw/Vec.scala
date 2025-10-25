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

import scala.util.hashing.MurmurHash3

case class Vec[T](arr: Array[T]) {
  def dimension: Int = arr.length

  override def hashCode(): Int = MurmurHash3.arrayHash(arr)

  override def toString: String = arr.mkString("Array(", ", ", ")")
}

object Vec {

  def equals[T](u: Vec[T], v: Vec[T])(implicit ord: Ordering[T]): Boolean = {
    if (u.dimension == v.dimension) {
      for (i <- u.arr.indices) {
        if (u.arr(i) != v.arr(i)) {
          return false
        }
      }
      true
    } else {
      false
    }
  }

  def ordering[T]()(implicit ord: Ordering[T]): Ordering[Vec[T]] = new Ordering[Vec[T]] {

    override def compare(u: Vec[T], v: Vec[T]): Int = {
      Utils.assert(u.dimension == v.dimension, s"u dim=${u.dimension}, v dim=${v.dimension}")
      for (i <- u.arr.indices) {
        val cmp = ord.compare(u.arr(i), v.arr(i))
        if (cmp != 0) {
          return cmp
        }
      }
      0
    }
  }
}
