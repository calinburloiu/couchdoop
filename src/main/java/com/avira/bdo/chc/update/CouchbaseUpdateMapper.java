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

package com.avira.bdo.chc.update;

import com.avira.bdo.chc.ArgsException;
import com.avira.bdo.chc.exp.CouchbaseAction;
import com.avira.bdo.chc.exp.ExportArgs;
import com.couchbase.client.CouchbaseClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This Mapper class is meant to update Couchbase documents, by using data from a configurable InputFormat.
 *
 * <p>The Couchbase keys should be read from the configured InputFormat. Any other data read from that source
 * can be combined with current document values from Couchbase in order to compute the values.</p>
 *
 * <p>Extensions of this class should implement </p>
 */
public abstract class CouchbaseUpdateMapper<KEYIN, VALUEIN, T> extends Mapper<KEYIN, VALUEIN, String, CouchbaseAction> {

  protected CouchbaseClient couchbaseClient;
  private Consumer consumer;

  private BlockingQueue<HadoopInput<T>> queue;

  private int bulkSize;

  private long putTimesSum = 0;

  public static final String PROPERTY_QUEUE_SIZE = "couchbase.update.queue.size";
  public static final String PROPERTY_BULK_SIZE = "couchbase.update.bulk.size";

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseUpdateMapper.class);

  public static enum Counters {QUEUE_PUT_TIMES_SUM, QUEUE_TAKE_TIMES_SUM, BULK_SIZES_SUM, BULKS_COUNT }

  public static class HadoopInput<T> {
    private String couchbaseKey;
    private T hadoopData;

    public HadoopInput(String couchbaseKey, T hadoopData) {
      set(couchbaseKey, hadoopData);
    }

    public void set(String couchbaseKey, T hadoopData) {
      this.couchbaseKey = couchbaseKey;
      this.hadoopData = hadoopData;
    }

    public String getCouchbaseKey() {
      return couchbaseKey;
    }

    public void setCouchbaseKey(String couchbaseKey) {
      this.couchbaseKey = couchbaseKey;
    }

    public T getHadoopData() {
      return hadoopData;
    }

    public void setHadoopData(T hadoopData) {
      this.hadoopData = hadoopData;
    }
  }

  public class Consumer extends Thread {

    private Context context;
    private boolean on;

    private long bulksCount = 0;
    private long bulkSizesSum = 0;
    private long takeTimesSum = 0;

    public Consumer(Context context) {
      this.context = context;
      this.on = true;
    }

    @Override
    public void run() {
      Collection<HadoopInput<T>> inputs = new ArrayList<HadoopInput<T>>(bulkSize);
      Collection<String> keys = new ArrayList<String>(bulkSize);
      Map<String, Object> docs;
      CouchbaseAction action;
      long t0, t1;

      while (on) {
        // Clean up.
        inputs.clear();
        keys.clear();

        try {
          t0 = System.currentTimeMillis();
          // Block until at least one element is available.
          inputs.add(queue.take());
          t1 = System.currentTimeMillis();
          takeTimesSum += t1 - t0;

          // Drain more elements from the queue if available, the more the better for the bulk get.
          queue.drainTo(inputs, bulkSize - 1);
          bulksCount++;
          bulkSizesSum += inputs.size();

          // Map the inputs to extract couchbase keys.
          for (HadoopInput<T> input : inputs) {
            keys.add(input.getCouchbaseKey());
          }

          // Do a bulk get for the keys read from Hadoop.
          docs = couchbaseClient.getBulk(keys);

          for (HadoopInput<T> input : inputs) {
            String key = input.getCouchbaseKey();

            // Compute the Couchbase operation and the new output document.
            action = merge(input.getHadoopData(), docs.get(key), context);

            // Write the newly updated document back to Couchbase.
            try {
              context.write(key, action);
            } catch (IOException e) {
              break;
            }
          }
        } catch (InterruptedException e) {
          // The thread end if interrupted.
          break;
        }
      }
    }

    public synchronized void shutdown() {
      on = false;
    }

    public synchronized long getBulksCount() {
      return bulksCount;
    }

    public synchronized long getBulkSizesSum() {
      return bulkSizesSum;
    }

    public synchronized long getTakeTimesSum() {
      return takeTimesSum;
    }
  }

  protected abstract HadoopInput<T> transform(KEYIN hKey, VALUEIN hValue, Context context);

  protected abstract CouchbaseAction merge(T t, Object cbInputValue, Context context);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    ExportArgs args;
    try {
      args = new ExportArgs(conf);
    } catch (ArgsException e) {
      throw new IllegalArgumentException(e);
    }

    // Create and configure queue.
    int queueSize = conf.getInt(PROPERTY_QUEUE_SIZE, 4096);
    bulkSize = conf.getInt(PROPERTY_BULK_SIZE, 1024);
    queue = new LinkedBlockingQueue<HadoopInput<T>>(queueSize);

    LOGGER.info("Connecting to Couchbase...");
    couchbaseClient = new CouchbaseClient(args.getUrls(), args.getBucket(), args.getPassword());
    LOGGER.info("Connected to Couchbase.");

    // Start the consumer thread.
    LOGGER.info("Starting consumer thread...");
    consumer = new Consumer(context);
    consumer.start();
    LOGGER.info("Consumer thread started.");
  }

  @Override
  protected void map(KEYIN hKey, VALUEIN hValue, Context context) throws IOException, InterruptedException {
    long t0, t1;

    // Transform the data received from the InputFormat.
    HadoopInput<T> hadoopInput = transform(hKey, hValue, context);
    if (hadoopInput == null) {
      return;
    }

    t0 = System.currentTimeMillis();
    // Put processed input into the producer-consumer queue. Wait if the queue is full.
    queue.put(hadoopInput);
    t1 = System.currentTimeMillis();
    putTimesSum += t1 - t0;
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    LOGGER.info("Disconnecting from Couchbase...");
    couchbaseClient.shutdown();

    LOGGER.info("Stopping consumer thread...");
    consumer.shutdown();

    // Update counters.
    context.getCounter(Counters.QUEUE_PUT_TIMES_SUM).setValue(putTimesSum);
    context.getCounter(Counters.BULKS_COUNT).setValue(consumer.getBulksCount());
    context.getCounter(Counters.BULK_SIZES_SUM).setValue(consumer.getBulkSizesSum());
    context.getCounter(Counters.QUEUE_TAKE_TIMES_SUM).setValue(consumer.getTakeTimesSum());
  }
}
