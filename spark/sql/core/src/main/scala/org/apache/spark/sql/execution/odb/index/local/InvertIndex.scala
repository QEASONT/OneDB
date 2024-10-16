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
package org.apache.spark.sql.execution.odb.index.local

import org.apache.spark.sql.catalyst.expressions.odb.ODBSimilarityFunction
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.ODBSimilarity
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point

class InvertedIndex(documents: Array[Point[Any]]) {

  // build inverted index
  val maxDist = documents.head.metricMaxDis
  documents.zipWithIndex.map {
    case (doc, id) => (id, doc.coord.asInstanceOf[String])
  }

  private val documentsWithId: Array[(Int, Long, String)] = documents.zipWithIndex.map {
    case (docPoint, id) => (id, docPoint.id, docPoint.coord.asInstanceOf[String])
  }
  private val wordsWithDocId: Array[(String, Int, Long)] = documentsWithId.flatMap {
    case (docId, pointId, content) =>
      content.split("""\W+""").filter(_.length > 2).map(word => (word.toLowerCase, docId, pointId))
  }

  private val invertedIndex: Map[String, Set[(Int, Long)]] = wordsWithDocId
    .groupBy(_._1)
    .mapValues(_.map(x => (x._2, x._3)).toSet)

  private def editDistance(p1: Any, p2: Any): Double = {
    val dist = ODBSimilarity.getDistanceFunction(ODBSimilarityFunction.EDIT).evalWithPoint(p1, p2) / maxDist
    dist
  }

  // KNN
  def knnSearch(query: String, k: Int): Array[((Int, Long), Double)] = {

    val candidateDocIds = query.split("""\W+""").flatMap { term =>
      invertedIndex.getOrElse(term.toLowerCase, Set.empty[(Int, Long)])
    }.toSet

    // calculate the edit distance between query and each word in the document
    val docIdToDistance = candidateDocIds.map { docId =>
      val docContent = documentsWithId(docId._1)._3
      val words = docContent.toLowerCase
      val cc = editDistance(query.toLowerCase, words)
      (docId, cc)
    }.toArray

    // sort by distance and take the top k
    docIdToDistance.sortBy(_._2).take(k)
  }

  // range search
  def rangeSearch(query: String, range: Double): Array[((Int, Long), Double)] = {
    // get the document IDs that contain words in the query
    val candidateDocIds = query.split("""\W+""").flatMap { term =>
      invertedIndex.getOrElse(term.toLowerCase, Set.empty[(Int, Long)])
    }.toSet

    // calculate the edit distance between query and each word in the document
    val docIdToDistance = candidateDocIds.map { docId =>
      val docContent = documentsWithId(docId._1)._3
      val words = docContent.toLowerCase()
      val cc = editDistance(query.toLowerCase, words)
      (docId, cc)
    }.toArray

    // filter by range
    docIdToDistance.filter(_._2 <= range).sortBy(_._2)
  }

  // print inverted index
  def printInvertedIndex(): Unit = {
    invertedIndex.foreach {
      case (word, docIds) =>
        println(s"$word: ${docIds.mkString(", ")}")
    }
  }
}
