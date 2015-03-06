package com.avira.couchdoop.spark

import com.avira.couchdoop.exp.{CouchbaseAction, CouchbaseOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Extra functions available on RDDs to be used as Couchbase output, of `(String, com.avira.couchdoop.exp.CouchbaseAction)`
 * pairs through an implicit conversion.
 * Import `com.avira.couchdoop.spark.CouchdoopSpark._` at the top of your program to use these functions.
 */
class CouchdoopRDDFunctions(self: RDD[(String, CouchbaseAction)]) {

  /**
   * Save the RDD to Couchbase by using the first element of the pair as key and the second element,
   * the `CouchbaseAction`, as the operation and the value (document). The implicit parameter is a configuration case
   * class necessary for Couchdoop to connect to Couchbase.
   */
  def saveToCouchbase(implicit cbOutputConf: CouchdoopExportConf) {
    val hadoopInitConf = cbOutputConf.hadoopConf.getOrElse(self.context.hadoopConfiguration)
    val hadoopJob = Job.getInstance(hadoopInitConf)
    CouchbaseOutputFormat.initJob(hadoopJob, cbOutputConf.urls.mkString(","),
      cbOutputConf.bucket, cbOutputConf.password)
    val hadoopConf = hadoopJob.getConfiguration

    self.saveAsNewAPIHadoopDataset(hadoopConf)
  }

  /**
   * Save the RDD to Couchbase by using the first element of the pair as key and the second element,
   * the `CouchbaseAction`, as the operation and the value (document). The three parameters are necessary to Couchdoop
   * to connect to Couchbase.
   */
  def saveToCouchbase(urls: Seq[String], bucket: String, password: String) {
    saveToCouchbase(CouchdoopExportConf(urls, bucket, password))
  }
}
