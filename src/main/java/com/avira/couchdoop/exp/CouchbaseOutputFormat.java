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

package com.avira.couchdoop.exp;

import com.avira.couchdoop.ArgsException;
import com.avira.couchdoop.CouchbaseArgs;
import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.internal.OperationFuture;
import org.apache.hadoop.conf.Configuration;
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
 * <p>Keys received correspond to the Couchbase keys and values received to Couchbase
 * documents.</p>
 */
public class CouchbaseOutputFormat extends OutputFormat<String, CouchbaseAction> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseOutputFormat.class);

  /**
   * How many times should failed operation be retried before ignoring the failure.
   *
   * If either this value or couchdoop.expBackoff.maxRetryIntervalPerTask is reached the failure is
   * ignored.
   */
  public final static String CONF_EXP_BACKOFF_MAX_TRIES_PER_TASK =
      "couchdoop.expBackoff.maxTriesPerTask";
  protected final static int EXP_BACKOFF_MAX_TRIES = 16;

  /**
   * Maximum number of milliseconds to wait until retrying a failed operation.
   *
   * If either this value or couchdoop.expBackoff.maxTriesPerTask is reached the failure is
   * ignored.
   */
  public final static String CONF_EXP_BACKOFF_MAX_RETRY_INTERVAL_PER_TASK =
      "couchdoop.expBackoff.maxRetryIntervalPerTask";
  protected final static int EXP_BACKOFF_MAX_RETRY_INTERVAL = 1000; // ms

  /**
   * Maximum total time in milliseconds for a task to wait until retrying failed operations.
   *
   * A task is failed if this value is reached.
   */
  public final static String CONF_EXP_BACKOFF_MAX_TOTAL_TIMEOUT_PER_TASK =
      "couchdoop.expBackoff.maxTotalTimeoutPerTask";
  protected final static int EXP_BACKOFF_MAX_TOTAL_TIMEOUT = 60000; // ms

  public static class CouchbaseRecordWriter extends RecordWriter<String, CouchbaseAction> {
    
    private CouchbaseClient couchbaseClient;

    private long nonExistentTouchedKeys = 0;
    private long failedStoreOperations = 0;
    private long timeoutOperations = 0;
    private long totalTimeout = 0;
    private long existentKeys = 0;
    private int[] expBackoffCounters;

    protected int expBackoffMaxTries;
    protected int expBackoffMaxRetryInterval; // ms
    protected int expBackoffMaxTotalTimeout; // ms

    public CouchbaseRecordWriter(List<URI> urls, String bucket, String password)
        throws IOException {
      LOGGER.info("Connecting to Couchbase bucket {} by using URLs {}...", bucket, urls, password);
      couchbaseClient = new CouchbaseClient(urls, bucket, password);
      LOGGER.info("Connected to Couchbase.");

      expBackoffCounters = new int[expBackoffMaxTries];
    }

    protected OperationFuture<Boolean> store(CouchbaseOperation operation,
                                             String key, Object value, int expiry) {
      switch (operation) {
        case SET:
          return couchbaseClient.set(key, expiry, value);
        case ADD:
          return couchbaseClient.add(key, expiry, value);
        case REPLACE:
          return couchbaseClient.replace(key, expiry, value);
        case APPEND:
          return couchbaseClient.append(key, value);
        case PREPEND:
          return couchbaseClient.prepend(key, value);
        case DELETE:
          return couchbaseClient.delete(key);
        case TOUCH:
          return couchbaseClient.touch(key, expiry);
        case EXISTS:
          return couchbaseClient.touch(key, expiry);
        default:
          // Ignore this action.
          return null;
      }
    }

    @Override
    public void write(String key, CouchbaseAction value) throws IOException, InterruptedException {
      int backoffExp = 0;
      OperationFuture<Boolean> future;

      // Store in Couchbase by doing exponential back-off.
      do {
        future = store(value.getOperation(), key, value.getValue(), value.getExpiry());
        if (future == null) {
          return;
        }

        // If the operation exists, count non existent touched keys.
        boolean res;
        try {
          res = future.get();
        } catch (Exception e) {
          res = false;

          if (e.getCause() instanceof CheckedOperationTimeoutException) {
            timeoutOperations++;
          }
        }
        if (!res && value.getOperation().equals(CouchbaseOperation.TOUCH)) {
          nonExistentTouchedKeys++;
        }

        if (!res) {
          failedStoreOperations++;
          if (future.getStatus().getMessage().contains("exists")) {
            existentKeys++;
          }
        }

        if (future.getStatus().isSuccess()
            || !future.getStatus().getMessage().equals("Temporary failure")
            || backoffExp >= expBackoffMaxTries) {
          break;
        }

        int retryInterval = Math.min((int) Math.pow(2, backoffExp), expBackoffMaxRetryInterval);
        Thread.sleep(retryInterval);
        backoffExp++;
        totalTimeout += retryInterval;
        expBackoffCounters[backoffExp]++;

        // A task is only allowed to stay blocked for a maximum amount of time.
        if (totalTimeout >= EXP_BACKOFF_MAX_TOTAL_TIMEOUT) {
          throw new IOException("couchdoop.expBackoff.maxTotalTimeoutPerTask was reached!");
        }
      } while (true);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      LOGGER.info("Disconnecting from Couchbase...");
      couchbaseClient.shutdown();

      if (failedStoreOperations > 0) {
        context.getCounter(CouchbaseOutputFormat.class.getName(), "FAILED_STORE_OPERATIONS")
            .increment(failedStoreOperations);
      }
      if (timeoutOperations > 0) {
        context.getCounter(CouchbaseOutputFormat.class.getName(), "TIMEOUT_OPERATIONS")
            .increment(timeoutOperations);
      }
      if (totalTimeout > 0) {
        context.getCounter(CouchbaseOutputFormat.class.getName(), "TOTAL_TIMEOUT")
            .increment(totalTimeout);
      }
      if (existentKeys > 0) {
        context.getCounter(CouchbaseOutputFormat.class.getName(), "EXISTENT_KEYS")
            .increment(existentKeys);
      }

      // Set counter for non existent touched keys if applicable.
      if (nonExistentTouchedKeys > 0) {
        context.getCounter(CouchbaseOutputFormat.class.getName(), "NON_EXISTENT_TOUCHED_KEYS")
            .increment(nonExistentTouchedKeys);
      }
      // Set counters for exponential back-off.
      for (int i = 0; i < expBackoffCounters.length; i++) {
        int expBackoffCounter = expBackoffCounters[i];
        if (expBackoffCounter > 0) {
          context.getCounter(CouchbaseOutputFormat.class.getName(),
              "EXP_BACKOFF_COUNT_FOR_TRY_" + i).increment(expBackoffCounter);
        }
      }
    }

    public void setExpBackoffMaxTries(int expBackoffMaxTries) {
      this.expBackoffMaxTries = expBackoffMaxTries;
    }

    public void setExpBackoffMaxRetryInterval(int expBackoffMaxRetryInterval) {
      this.expBackoffMaxRetryInterval = expBackoffMaxRetryInterval;
    }

    public void setExpBackoffMaxTotalTimeout(int expBackoffMaxTotalTimeout) {
      this.expBackoffMaxTotalTimeout = expBackoffMaxTotalTimeout;
    }
  }

  public static void initJob(Job job, String urls, String bucket, String password) {
    job.setOutputFormatClass(CouchbaseOutputFormat.class);
    job.setOutputKeyClass(String.class);
    job.setOutputValueClass(CouchbaseAction.class);

    Configuration conf = job.getConfiguration();
    conf.set(CouchbaseArgs.ARG_COUCHBASE_URLS.getPropertyName(), urls);
    conf.set(CouchbaseArgs.ARG_COUCHBASE_BUCKET.getPropertyName(), bucket);
    conf.set(CouchbaseArgs.ARG_COUCHBASE_PASSWORD.getPropertyName(), password);
  }

  public RecordWriter<String, CouchbaseAction> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    ExportArgs args;
    try {
      args = new ExportArgs(conf);
    } catch (ArgsException e) {
      throw new IllegalArgumentException(e);
    }

    CouchbaseRecordWriter couchbaseRecordWriter =
        new CouchbaseRecordWriter(args.getUrls(), args.getBucket(), args.getPassword());

    couchbaseRecordWriter.setExpBackoffMaxTries(
        conf.getInt(CONF_EXP_BACKOFF_MAX_TRIES_PER_TASK, EXP_BACKOFF_MAX_TRIES)
    );
    couchbaseRecordWriter.setExpBackoffMaxRetryInterval(
        conf.getInt(CONF_EXP_BACKOFF_MAX_RETRY_INTERVAL_PER_TASK, EXP_BACKOFF_MAX_RETRY_INTERVAL)
    );
    couchbaseRecordWriter.setExpBackoffMaxTotalTimeout(
        conf.getInt(CONF_EXP_BACKOFF_MAX_TOTAL_TIMEOUT_PER_TASK, EXP_BACKOFF_MAX_TOTAL_TIMEOUT)
    );

    return couchbaseRecordWriter;
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {}

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
  }
}
