package com.avira.bdo.chc.exp;

import com.avira.bdo.chc.ArgsException;
import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * This output format writes writes each key-value received as a Couchbase document.
 *
 * <p>Keys received correspond to the Couchbase keys and values received to Couchbase documents.</p>
 */
public class CouchbaseOutputFormat extends OutputFormat<String, CouchbaseAction> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseOutputFormat.class);

  public static class CouchbaseRecordWriter extends RecordWriter<String, CouchbaseAction> {
    
    private CouchbaseClient couchbaseClient;

    private long nonExistentTouchedKeys = 0;
    private int[] expBackoffCounters;

    protected final static int EXP_BACKOFF_MAX_TRIES = 16;
    protected final static int EXP_BACKOFF_MAX_RETRY_INTERVAL = 1000; // ms

    public CouchbaseRecordWriter(List<URI> urls, String bucket, String password) throws IOException {
      LOGGER.info("Connecting to Couchbase...");
      couchbaseClient = new CouchbaseClient(urls, bucket, password);
      LOGGER.info("Connected to Couchbase.");

      expBackoffCounters = new int[EXP_BACKOFF_MAX_TRIES];
    }

    protected OperationFuture<Boolean> store(CouchbaseOperation operation, String key, Object value) {
      switch (operation) {
        case SET:
          return couchbaseClient.set(key, value);
        case ADD:
          return couchbaseClient.add(key, value);
        case REPLACE:
          return couchbaseClient.replace(key, value);
        case APPEND:
          return couchbaseClient.append(key, value);
        case PREPEND:
          return couchbaseClient.prepend(key, value);
        case DELETE:
          return couchbaseClient.delete(key);
        case EXISTS:
          return couchbaseClient.touch(key, 0);
        default:
          // Ignore this action.
          return null;
      }
    }

    @Override
    public void write(String key, CouchbaseAction value) throws IOException, InterruptedException {
      int backoffExp = 0;
      OperationFuture<Boolean> future;
      OperationStatus status;

      // Store in Couchbase by doing exponential back-off.
      try {

        do {
          future = store(value.getOperation(), key, value.getValue());
          status = future.getStatus();

          if (status.isSuccess() || !status.getMessage().equals("Temporary failure") ||
              backoffExp >= EXP_BACKOFF_MAX_TRIES) {
            break;
          }

          int retryInterval = Math.min((int) Math.pow(2, backoffExp), 1000);
          Thread.sleep(retryInterval);
          expBackoffCounters[backoffExp]++;

          backoffExp++;
        } while (status.getMessage().equals("Temporary failure"));

        boolean res = future.get();
        // If the operation is exists, count non existent touched keys.
        if (!res && value.getOperation().equals(CouchbaseOperation.EXISTS)) {
          nonExistentTouchedKeys++;
        }
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      LOGGER.info("Disconnecting from Couchbase...");
      couchbaseClient.shutdown();

      // Set counter for non existent touched keys if applicable.
      if (nonExistentTouchedKeys > 0) {
        context.getCounter(CouchbaseOutputFormat.class.getName(), "NON_EXISTENT_TOUCHED_KEYS").increment(nonExistentTouchedKeys);
      }
      // Set counters for exponential back-off.
      for (int i = 0; i < expBackoffCounters.length; i++) {
        int expBackoffCounter = expBackoffCounters[i];
        if (expBackoffCounter > 0) {
          context.getCounter(CouchbaseOutputFormat.class.getName(), "EXP_BACKOFF_COUNT_FOR_TRY_" + i).increment(expBackoffCounter);
        }
      }
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
