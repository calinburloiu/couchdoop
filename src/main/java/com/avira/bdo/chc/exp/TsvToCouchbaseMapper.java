package com.avira.bdo.chc.exp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This mapper maps key-value pairs read from TSV files as documents in Couchbase by using keys as IDs and values as
 * documents.
 */
public class TsvToCouchbaseMapper extends Mapper<LongWritable, Text, Text, Text> {

  private static final String COUNTER_ERRORS = "ERRORS";

  enum Error { LINES_WITH_WRONG_COLUMNS_COUNT }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] pair = value.toString().split("\t");

    // Skip error line.
    if (pair.length != 2) {
      context.getCounter(Error.LINES_WITH_WRONG_COLUMNS_COUNT).increment(1);
      return;
    }

    Text docId = new Text(pair[0]);
    Text doc = new Text(pair[1]);

    context.write(docId, doc);
  }
}
