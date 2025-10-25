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
package org.apache.spark.sql.execution.odb.index.local

import org.apache.spark.sql.catalyst.expressions.odb.ODBSimilarityFunction
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.ODBSimilarity
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point

import scala.collection.mutable

class InvertedIndex(var documents: Map[Long, Point[Any]]) {

  // Inverted index: word -> Set(pointId)
  private val invertedIndex: mutable.Map[String, mutable.Set[Long]] =
    mutable.Map.empty

  // Build inverted index for initial documents
  documents.values.foreach(indexDocument)

  // Maximum distance used for normalization
  private var maxDist: Double =
    if (documents.nonEmpty) documents.values.take(1).map(_.metricMaxDis).max else 1.0

  // Tokenize document content and update the inverted index
  private def indexDocument(docPoint: Point[Any]): Unit = {
    val pointId = docPoint.id
    val content = docPoint.coord.asInstanceOf[String]
    val words = content.split("""\W+""").filter(_.length > 2)
    words.foreach { w =>
      val word = w.toLowerCase
      invertedIndex.getOrElseUpdate(word, mutable.Set.empty) += pointId
    }
  }

  // Remove a document's words from the inverted index
  private def removeFromIndex(docPoint: Point[Any]): Unit = {
    val pointId = docPoint.id
    val content = docPoint.coord.asInstanceOf[String]
    val words = content.split("""\W+""").filter(_.length > 2).map(_.toLowerCase)
    words.foreach { w =>
      invertedIndex.get(w).foreach { ids =>
        ids -= pointId
        if (ids.isEmpty) invertedIndex -= w
      }
    }
  }

  // Compute normalized edit distance
  private def editDistance(p1: Any, p2: Any): Double = {
    val dist = ODBSimilarity
      .getDistanceFunction(ODBSimilarityFunction.EDIT)
      .evalWithPoint(p1, p2)
    val res = dist / maxDist
    res
  }

  // Insert a single document
  def insert(docPoint: Point[Any]): Unit = {
    documents += (docPoint.id -> docPoint)
    indexDocument(docPoint)
    maxDist = math.max(maxDist, docPoint.metricMaxDis)
  }

  // Insert multiple documents in batch (more efficient than repeated insert)
  def insertAll(points: Iterable[Point[Any]]): Unit = {
    if (points.nonEmpty) {
      documents ++= points.map(p => p.id -> p)
      points.foreach(indexDocument)
      val maxOfBatch = points.take(1).map(_.metricMaxDis).max
      maxDist = math.max(maxDist, maxOfBatch)
    }
  }

  // Delete a document by pointId
  def delete(pointId: Long): Unit = {
    documents.get(pointId).foreach { point =>
      removeFromIndex(point)
      documents -= pointId
    }
  }

  // K-Nearest Neighbor search
  def knnSearch(query: String, k: Int): Array[(Long, Double)] = {
    val candidateIds = query
      .split("""\W+""")
      .flatMap(term => invertedIndex.getOrElse(term.toLowerCase, Set.empty[Long]))
      .toSet

    val idToDistance = candidateIds.map { pid =>
      val docContent = documents(pid).coord.asInstanceOf[String].toLowerCase
      val dist = editDistance(query.toLowerCase, docContent)
      (pid, dist)
    }.toArray

    idToDistance.sortBy(_._2).take(k)
  }

  // Range search (all documents with distance <= threshold)
  def rangeSearch(query: String, range: Double): Array[(Long, Double)] = {
    val candidateIds = query
      .split("""\W+""")
      .flatMap(term => invertedIndex.getOrElse(term.toLowerCase, Set.empty[Long]))
      .toSet

    candidateIds
      .map { pid =>
        val docContent = documents(pid).coord.asInstanceOf[String].toLowerCase
        val dist = editDistance(query.toLowerCase, docContent)
        (pid, dist)
      }
      .filter(_._2 <= range)
      .toArray
      .sortBy(_._2)
  }

  // Print the inverted index (debugging / inspection only)
  def printInvertedIndex(): Unit = {
    invertedIndex.foreach {
      case (word, ids) =>
        println(s"$word: ${ids.mkString(", ")}")
    }
  }
}