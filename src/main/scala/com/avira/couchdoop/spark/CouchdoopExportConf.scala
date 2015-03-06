package com.avira.couchdoop.spark

import org.apache.hadoop.conf.Configuration

/**
 * Case class to hold configuration parameters for writing to Couchbase with Couchdoop.
 */
case class CouchdoopExportConf(
  urls: Seq[String],
  bucket: String,
  password: String,
  hadoopConf: Option[Configuration] = None
)