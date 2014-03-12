package com.avira.bdo.chc.test

import com.couchbase.client.CouchbaseClient
import collection.JavaConversions._
import play.api.libs.json._
import scala.annotation.tailrec
import java.io.PrintWriter

/**
 * This main object generates Couchbase test documents used for benchmarking.
 *
 * The documents have keys of format "update::${letters}" and values as JSON documents of format
 * {"letters":"${letters}","number":"${number}", where:
 *
 * * ${letters} have values "aaaaa", "aaaab", ..., "zzzzy", "zzzzz"
 * * ${number} has values 0, 1, ..., 456975.
 *
 * The limit parameter sets the limit letter for the first character in ${letters}.
 */
class CouchbaseBenchmarkDataGenerator(url: String, bucket: String, password: String, limit: Char, output: String = null) {
  import Math.pow

  lazy val letters = for (i0 <- 'a' to limit; i1 <- 'a' to 'z'; i2 <- 'a' to 'z'; i3 <- 'a' to 'z'; i4 <- 'a' to 'z')
      yield s"$i0$i1$i2$i3$i4"
  val values = letters.map { x => Json.obj("letters" -> x, "number" -> letters2number(x)) }
  val keys = letters.map { x => s"update::$x" }

  lazy val couchbaseClient = new CouchbaseClient(Seq(java.net.URI.create(url)), bucket, password)

  val hdfsChunkSize = 456976

  case class Bean(letters: String, number: Int)
  implicit val beanReads: Reads[Bean] = Json.reads[Bean]

  def letters2number(l: String): Int = ((l(0)-'a')*pow(26, 3) + (l(1)-'a')*pow(26, 2) + (l(2)-'a')*26 + (l(3)-'a')).toInt

  def writeToCouchbase() {
    keys zip values foreach { case (k, v) =>
      couchbaseClient.set(k, v.toString()).get()
    }
  }

  def writeToHdfs() {
    @tailrec
    def writeChunk(chunk: Seq[(String, JsObject)], remaining: Seq[(String, JsObject)], page: Int) {
      if (chunk.nonEmpty) {
        val writer = new PrintWriter(output + "/part-" + page)
        chunk foreach { case (k, v) => writer.println(k + "\t" + v) }
        writer.close()
      }

      if (remaining.nonEmpty) {
        val (newChunk, newRemaining) = remaining.splitAt(hdfsChunkSize)
        writeChunk(newChunk, newRemaining, page + 1)
      }
    }

    writeChunk(Seq(), keys.zip(values), -1)
  }

  def check(keysToCheck: Seq[String], positive: Boolean): List[String] = {
    keysToCheck.foldLeft(List[String]()) { (acc: List[String], key: String) =>
      val rawValue = couchbaseClient.get(key)
      if (rawValue != null) {
        val value = Json.parse(rawValue.toString)
        val bean = Json.fromJson[Bean](value).get

        if (positive) {
          if (bean.letters(0).isLower && bean.number >= 0) acc else key :: acc
        } else {
          if (bean.letters(0).isUpper && bean.number <= 0) acc else key :: acc
        }
      } else {
        key :: acc
      }
    }
  }

  def close() {
    couchbaseClient.shutdown()
  }
}
