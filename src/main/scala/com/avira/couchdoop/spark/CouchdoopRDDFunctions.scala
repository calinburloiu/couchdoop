/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.avira.couchdoop.spark

import com.avira.couchdoop.exp.{CouchbaseAction, CouchbaseOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

/**
 * Extra functions available on RDDs to be used as Couchbase output, of
 * `(String, com.avira.couchdoop.exp.CouchbaseAction)` pairs through an implicit conversion.
 * Import `com.avira.couchdoop.spark.CouchdoopSpark._` at the top of your program to use these
 * functions.
 */
class CouchdoopRDDFunctions(self: RDD[(String, CouchbaseAction)]) {

  /**
   * Save the RDD to Couchbase by using the first element of the pair as key and the second element,
   * the `CouchbaseAction`, as the operation and the value (document). The implicit parameter is a
   * configuration case class necessary for Couchdoop to connect to Couchbase.
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
   * the `CouchbaseAction`, as the operation and the value (document). The three parameters are
   * necessary to Couchdoop to connect to Couchbase.
   */
  def saveToCouchbase(urls: Seq[String], bucket: String, password: String) {
    saveToCouchbase(CouchdoopExportConf(urls, bucket, password))
  }
}
