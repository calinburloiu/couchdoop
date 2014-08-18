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

package com.avira.couchdoop.imp;

import com.avira.couchdoop.ArgsException;
import com.avira.couchdoop.CouchbaseArgs;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;

/**
 * This input format reads documents from a Couchbase view queried by a list of view keys.
 * <p>
 * Instances emit document IDs as key and the corresponding Couchbase {@link com.couchbase.client.protocol.views.ViewRow}
 * as value.
 * <p>
 * The view keys passed as input are distributed evenly across a configurable number of Mapper tasks.
 */
public class CouchbaseViewInputFormat extends InputFormat<Text, ViewRow> {

  public static class CouchbaseViewInputSplit extends InputSplit implements Writable {

    private String viewKey;

    /**
     * Default constructor (necessary because this is a Writable)
     */
    public CouchbaseViewInputSplit() {
    }

    /**
     * Convenience constructor
     * @param viewKey Couchbase view key that is queried for documents
     */
    public CouchbaseViewInputSplit(String viewKey) {
      this.viewKey = viewKey;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      // The split size is not calculated for performance reasons.
      return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      // It is assumed that the Couchbase nodes cannot be local.
      return new String[] {};
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(viewKey);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      viewKey = in.readUTF();
    }

    public String getViewKey() {
      return viewKey;
    }
  }

  public static class CouchbaseViewRecordReader extends RecordReader<Text, ViewRow> {

    private CouchbaseClient couchbaseClient;
    private Paginator pages;
    private Iterator<ViewRow> rowIterator;

    private Text key;
    private ViewRow value;

    private boolean finished = false;

    public static final int CONNECT_RETRIES = 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseViewRecordReader.class);

    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      CouchbaseArgs couchbaseArgs;
      ImportViewArgs importViewArgs;
      try {
        couchbaseArgs = new CouchbaseArgs(conf);
        importViewArgs = new ImportViewArgs(conf);
      } catch (ArgsException e) {
        throw new IllegalArgumentException(e);
      }

      // Check args presence.
      if (couchbaseArgs.getUrls() == null || couchbaseArgs.getBucket() == null ||
          couchbaseArgs.getPassword() == null) {
        throw new IllegalArgumentException("Not all Couchbase arguments are present: " + couchbaseArgs);
      }

      // Connect to Couchbase. Retry a few times on cancellation.
      int retryNo;
      for (retryNo = 1; retryNo <= CONNECT_RETRIES; retryNo++) {
        couchbaseClient =
          connectToCouchbase(couchbaseArgs.getUrls(), couchbaseArgs.getBucket(), couchbaseArgs.getPassword());

        // Initialize fields.
        CouchbaseViewInputSplit couchbaseViewInputSplit = (CouchbaseViewInputSplit) inputSplit;
        key = new Text();

        // Prepare for querying the Couchbase view.
        LOGGER.info("Querying Couchbase for view {}...");
        View view = couchbaseClient.getView(importViewArgs.getDesignDocumentName(), importViewArgs.getViewName());
        Query query = new Query();
        query.setKey(couchbaseViewInputSplit.getViewKey());
        query.setIncludeDocs(true);
        pages = couchbaseClient.paginatedQuery(view, query, importViewArgs.getDocumentsPerPage());

        try {
          if (pages.hasNext()) {
            ViewResponse viewResponse = pages.next();
            rowIterator = viewResponse.iterator();
          }
        } catch (CancellationException e) {
          LOGGER.error("Attempt no. {} to connect to Couchbase for querying view {} failed due to: {}",
              retryNo, couchbaseViewInputSplit.getViewKey(), ExceptionUtils.getFullStackTrace(e));
          couchbaseClient.shutdown();
          continue;
        }

        // Connection was successfully established because no exception was thrown.
        break;
      }

      if (retryNo >= CONNECT_RETRIES) {
        throw new RuntimeException("Failed to connect to Couchbase after " + CONNECT_RETRIES +
            " retries.");
      }
    }

    private CouchbaseClient connectToCouchbase(List<URI> urls, String bucket, String password) throws IOException {
      CouchbaseClient couchbaseClient;

      LOGGER.info("Connecting to Couchbase...");
      try {
        couchbaseClient = new CouchbaseClient(urls, bucket, password);
        LOGGER.info("Connected to Couchbase.");
      } catch (IOException e) {
        LOGGER.error(ExceptionUtils.getStackTrace(e));
        throw e;
      }

      return couchbaseClient;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (nextRow()) {
        return true;
      } else {
        if (pages.hasNext()) {
          ViewResponse viewResponse = pages.next();
          rowIterator = viewResponse.iterator();
          return nextRow();
        } else {
          finished = true;
          return false;
        }
      }
    }

    private boolean nextRow() {
      if (rowIterator == null) {
        return false;
      }

      if (rowIterator.hasNext()) {
        value = rowIterator.next();
        key.set(value.getId());

        return true;
      }

      return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    @Override
    public ViewRow getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      if (finished) {
        return 1.0f;
      }

      return 0.0f;
    }

    @Override
    public void close() throws IOException {
      LOGGER.info("Disconnecting from Couchbase...");
      couchbaseClient.shutdown();
    }
  }

  public static void initJob(Job job, String urls, String bucket, String password,
                             String designDocumentName, String viewName, String viewKeys) {
    job.setInputFormatClass(CouchbaseViewInputFormat.class);

    // User classpath takes precedence in favor of Hadoop classpath.
    // This is because the Couchbase client requires a newer version of
    // org.apache.httpcomponents:httpcore.
    job.setUserClassesTakesPrecedence(true);

    Configuration conf = job.getConfiguration();
    conf.set(CouchbaseArgs.ARG_COUCHBASE_URLS.getPropertyName(), urls);
    conf.set(CouchbaseArgs.ARG_COUCHBASE_BUCKET.getPropertyName(), bucket);
    conf.set(CouchbaseArgs.ARG_COUCHBASE_PASSWORD.getPropertyName(), password);
    conf.set(ImportViewArgs.ARG_DESIGNDOC_NAME.getPropertyName(), designDocumentName);
    conf.set(ImportViewArgs.ARG_VIEW_NAME.getPropertyName(), viewName);
    conf.set(ImportViewArgs.ARG_VIEW_KEYS.getPropertyName(), viewKeys);
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    List<InputSplit> inputSplits = new ArrayList<InputSplit>();
    String[] viewKeys = ImportViewArgs.parseViewKeys(jobContext.getConfiguration());

    for (String viewKey : viewKeys) {
      inputSplits.add(new CouchbaseViewInputSplit(viewKey));
    }

    return inputSplits;
  }

  @Override
  public RecordReader<Text, ViewRow> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new CouchbaseViewRecordReader();
  }
}
