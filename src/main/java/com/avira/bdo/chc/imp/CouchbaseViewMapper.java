package com.avira.bdo.chc.imp;

import com.couchbase.client.protocol.views.ViewRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class which maps each document retrieved from a Couchbase view key to text key-values.
 */
public class CouchbaseViewMapper extends Mapper<Text, ViewRow, Text, Text> {

  @Override
  protected void map(Text key, ViewRow value, Context context) throws IOException, InterruptedException {
    context.write(key, new Text(value.getDocument().toString()));
  }
}
