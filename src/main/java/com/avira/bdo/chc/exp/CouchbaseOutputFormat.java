package com.avira.bdo.chc.exp;

import com.avira.bdo.chc.ArgsException;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.StoreType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * This output format writes writes each key-value received as a Couchbase document.
 *
 * <p>Keys received correspond to the Couchbase keys and values received to Couchbase documents.</p>
 */
public class CouchbaseOutputFormat extends OutputFormat<Text, Writable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseOutputFormat.class);

  public static class CouchbaseRecordWriter extends RecordWriter<Text, Writable> {
    
    private CouchbaseClient couchbaseClient;

    private StoreType storeType = StoreType.SET;
    
    public CouchbaseRecordWriter(List<URI> urls, String bucket, String password, StoreType storeType) throws IOException {
      LOGGER.info("Connecting to Couchbase...");
      couchbaseClient = new CouchbaseClient(urls, bucket, password);
      LOGGER.info("Connected to Couchbase.");

      this.storeType = storeType;
    }

    @Override
    public void write(Text key, Writable value) throws IOException, InterruptedException {
      switch (storeType) {
        case SET:
          couchbaseClient.set(key.toString(), value.toString());
          break;
        case ADD:
          couchbaseClient.add(key.toString(), value.toString());
          break;
        case REPLACE:
          couchbaseClient.replace(key.toString(), value.toString());
          break;
        case APPEND:
          couchbaseClient.append(key.toString(), value.toString());
          break;
        case PREPEND:
          couchbaseClient.prepend(key.toString(), value.toString());
          break;
      }


    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      LOGGER.info("Disconnecting from Couchbase...");
      couchbaseClient.shutdown();
    }
  }

  public RecordWriter<Text, Writable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    ExportArgs args;
    try {
      args = new ExportArgs(context.getConfiguration());
    } catch (ArgsException e) {
      throw new IllegalArgumentException(e);
    }

    return new CouchbaseRecordWriter(args.getUrls(), args.getBucket(),
        args.getPassword(), args.getStoreType());
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {}

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
  }
}
