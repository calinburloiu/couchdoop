package com.avira.bdo.chc.exp;

import com.avira.bdo.chc.ArgsException;
import com.couchbase.client.CouchbaseClient;
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
public class CouchbaseOutputFormat extends OutputFormat<String, CouchbaseAction> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseOutputFormat.class);

  public static class CouchbaseRecordWriter extends RecordWriter<String, CouchbaseAction> {
    
    private CouchbaseClient couchbaseClient;

    public CouchbaseRecordWriter(List<URI> urls, String bucket, String password) throws IOException {
      LOGGER.info("Connecting to Couchbase...");
      couchbaseClient = new CouchbaseClient(urls, bucket, password);
      LOGGER.info("Connected to Couchbase.");
    }

    @Override
    public void write(String key, CouchbaseAction value) throws IOException, InterruptedException {
      switch (value.getOperation()) {
        case SET:
          couchbaseClient.set(key, value.getValue().toString());
          break;
        case ADD:
          couchbaseClient.add(key, value.getValue().toString());
          break;
        case REPLACE:
          couchbaseClient.replace(key, value.getValue().toString());
          break;
        case APPEND:
          couchbaseClient.append(key, value.getValue().toString());
          break;
        case PREPEND:
          couchbaseClient.prepend(key, value.getValue().toString());
          break;
        case DELETE:
          couchbaseClient.delete(key);
          break;
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      LOGGER.info("Disconnecting from Couchbase...");
      couchbaseClient.shutdown();
    }
  }

  public RecordWriter<String, CouchbaseAction> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    ExportArgs args;
    try {
      args = new ExportArgs(context.getConfiguration());
    } catch (ArgsException e) {
      throw new IllegalArgumentException(e);
    }

    return new CouchbaseRecordWriter(args.getUrls(), args.getBucket(),
        args.getPassword());
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {}

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
  }
}
