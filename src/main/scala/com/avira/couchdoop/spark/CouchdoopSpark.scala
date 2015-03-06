package com.avira.couchdoop.spark

import com.avira.couchdoop.exp.CouchbaseAction
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

/**
 * Utility implicit methods for use in Spark to provide integration with Couchbase.
 */
object CouchdoopSpark {

  /**
   * Implicit function used to provide `saveToCouchbase` methods to RDDs of of `(String, com.avira.couchdoop.exp.CouchbaseAction)`
   * pairs.
   */
  implicit def rddToCouchdoopRDDFunctions(rdd: RDD[(String, CouchbaseAction)]): CouchdoopRDDFunctions = {
    new CouchdoopRDDFunctions(rdd)
  }
}
