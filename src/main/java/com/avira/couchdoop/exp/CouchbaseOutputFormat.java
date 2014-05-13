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
import com.couchbase.client.CouchbaseClient;
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
    private long failedStoreOperations = 0;
    private long existentKeys = 0;
    private int[] expBackoffCounters;

    protected final static int EXP_BACKOFF_MAX_TRIES = 16;
    protected final static int EXP_BACKOFF_MAX_RETRY_INTERVAL = 1000; // ms

    public CouchbaseRecordWriter(List<URI> urls, String bucket, String password) throws IOException {
      LOGGER.info("Connecting to Couchbase...");
      couchbaseClient = new CouchbaseClient(urls, bucket, password);
      LOGGER.info("Connected to Couchbase.");

      expBackoffCounters = new int[EXP_BACKOFF_MAX_TRIES];
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
      OperationStatus status;

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
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
        if (!res && value.getOperation().equals(CouchbaseOperation.EXISTS)) {
          nonExistentTouchedKeys++;
        }

        // TODO See if false also means error.
        if (!res) {
          failedStoreOperations++;
          if (future.getStatus().getMessage().contains("exists")) {
            existentKeys++;
          }
        }

        if (future.getStatus().isSuccess() || !future.getStatus().getMessage().equals("Temporary failure") ||
            backoffExp >= EXP_BACKOFF_MAX_TRIES) {
          break;
        }

        int retryInterval = Math.min((int) Math.pow(2, backoffExp), EXP_BACKOFF_MAX_RETRY_INTERVAL);
        Thread.sleep(retryInterval);
        expBackoffCounters[backoffExp]++;

        backoffExp++;
      } while (true);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      LOGGER.info("Disconnecting from Couchbase...");
      couchbaseClient.shutdown();

      if (failedStoreOperations > 0) {
        context.getCounter(CouchbaseOutputFormat.class.getName(), "FAILED_STORE_OPERATIONS").increment(failedStoreOperations);
      }
      if (existentKeys > 0) {
        context.getCounter(CouchbaseOutputFormat.class.getName(), "EXISTANT_KEYS").increment(existentKeys);
      }

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
