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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * This input format reads documents from a Couchbase view queried by a list of view keys.
 * <p/>
 * Instances emit document IDs as key and the corresponding Couchbase {@link com.couchbase.client.protocol.views.ViewRow}
 * as value.
 * <p/>
 * The view keys passed as input are distributed evenly across a configurable number of Mapper tasks.
 */
public class CouchbaseViewInputFormat extends InputFormat<Text, ViewRow> {

  public static class CouchbaseViewInputSplit extends InputSplit implements Writable {

    private List<String> viewKeys = new ArrayList<>();

    /**
     * Default constructor (necessary because this is a Writable)
     */
    public CouchbaseViewInputSplit() {
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      // The split size is calculated only roughly from the number of keys
      // out of performance considerations
      return viewKeys.size();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      // It is assumed that the Couchbase nodes cannot be local.
      return new String[]{};
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text[] viewKeysTexts = new Text[viewKeys.size()];

      int i = 0;
      for (String viewKey : viewKeys) {
        viewKeysTexts[i++] = new Text(viewKey);
      }

      ArrayWritable viewKeysWritable = new ArrayWritable(Text.class, viewKeysTexts);
      viewKeysWritable.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      ArrayWritable viewKeysWritable = new ArrayWritable(Text.class);
      viewKeysWritable.readFields(in);

      Collections.addAll(viewKeys, viewKeysWritable.toStrings());
    }

    public void addKey(String key) {
      viewKeys.add(key);
    }

    public List<String> getKeys() {
      return viewKeys;
    }
  }

  public static class CouchbaseViewRecordReader extends RecordReader<Text, ViewRow> {

    private List<URI> couchbaseUrls;
    private String couchbaseBucket;
    private String couchbasePassword;
    private String couchbaseDesignDocName;
    private String couchbaseViewName;
    private int couchbaseDocsPerPage;

    private Queue<String> keyQueue = new LinkedList<>();
    private int totalNumKeys;

    private CouchbaseClient couchbaseClient;
    private View view;
    private Paginator paginator;
    private Iterator<ViewRow> rowIterator;

    private Text key = new Text();
    private ViewRow value;

    private boolean finished = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseViewRecordReader.class);

    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
      initCouchbaseArgs(context);

      //Add all keys to a queue
      CouchbaseViewInputSplit couchbaseViewInputSplit = (CouchbaseViewInputSplit) inputSplit;
      keyQueue.addAll(couchbaseViewInputSplit.getKeys());
      totalNumKeys = keyQueue.size();

      if (0 == keyQueue.size()) {
        //No keys
        return;
      }

      initCouchbaseClient();
      initCouchbaseView();
      initNextRowIterator();
    }


    private boolean initNextRowIterator() {
      if ( (paginator != null) && paginator.hasNext() ) {
        //We have a next page, get the row iterator from there
        rowIterator = paginator.next().iterator();
        return true;
      }

      String nextKey = keyQueue.poll();
      if(nextKey == null) {
        //No more keys, no more row iterators, return null
        return false;
      }

      Query query = getQueryForKey(nextKey);
      paginator = couchbaseClient.paginatedQuery(view, query, couchbaseDocsPerPage);

      //Loaded a new Paginator, start over
      return initNextRowIterator();
    }

    private Query getQueryForKey(String key) {
      Query query = new Query();
      query.setKey(key);
      query.setIncludeDocs(true);
      return query;
    }


    private void initCouchbaseView() throws IOException {
      // Prepare for querying the Couchbase view.
      LOGGER.info("Querying Couchbase for view {}...");
      view = couchbaseClient.getView(couchbaseDesignDocName, couchbaseViewName);
    }


    private void initCouchbaseArgs(TaskAttemptContext context) {
      Configuration conf = context.getConfiguration();
      ImportViewArgs importViewArgs;
      try {
        importViewArgs = new ImportViewArgs(conf);
      } catch (ArgsException e) {
        throw new IllegalArgumentException(e);
      }

      // Check args presence.
      if (importViewArgs.getUrls() == null || importViewArgs.getBucket() == null ||
        importViewArgs.getPassword() == null) {
        throw new IllegalArgumentException("Not all Couchbase arguments are present: " + importViewArgs);
      }

      couchbaseUrls = importViewArgs.getUrls();
      couchbaseBucket = importViewArgs.getBucket();
      couchbasePassword = importViewArgs.getPassword();
      couchbaseDesignDocName = importViewArgs.getDesignDocumentName();
      couchbaseViewName = importViewArgs.getViewName();
      couchbaseDocsPerPage = importViewArgs.getDocumentsPerPage();
    }


    private void initCouchbaseClient() throws IOException {
      //Skip if couchbaseClient is already set
      if(couchbaseClient != null) { return; }

      LOGGER.info("Connecting to Couchbase...");
      try {
        couchbaseClient = new CouchbaseClient(couchbaseUrls, couchbaseBucket, couchbasePassword);
        LOGGER.info("Connected to Couchbase.");
      } catch (IOException e) {
        LOGGER.error(ExceptionUtils.getStackTrace(e));
        throw e;
      }
    }

    private void disconnectFromCouchbase() {
      //Skip if couchbaseClient is not set
      if(couchbaseClient==null) { return; }
      LOGGER.info("Disconnecting from Couchbase...");
      couchbaseClient.shutdown();
      couchbaseClient = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      //If we can get a new row, we're fine
      if (nextRow()) {
        return true;
      }

      //Try to get a new rowIterator and make another attempt
      while(initNextRowIterator()) {
        if (nextRow()) {
          return true;
        }
      }

      return false;
    }

    private boolean nextRow() {
      if (rowIterator == null || !rowIterator.hasNext()) {
        return false;
      }
      value = rowIterator.next();
      key.set(value.getId());
      return true;
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
      return (float) ( (totalNumKeys - keyQueue.size()) / totalNumKeys );
    }

    @Override
    public void close() throws IOException {
      disconnectFromCouchbase();
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseViewInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    List<InputSplit> inputSplits = new ArrayList<InputSplit>();

    ImportViewArgs importViewArgs;
    try {
      importViewArgs = new ImportViewArgs(jobContext.getConfiguration());
    } catch (ArgsException e) {
      throw new RuntimeException("ImportViewArgs can't load settings from Hadoop Configuration");
    }
    String[] viewKeys = importViewArgs.getViewKeys();
    int viewKeysPerMapTask = (int) Math.ceil((double)viewKeys.length / importViewArgs.getNumMappers());

    LOGGER.info("Number of keys per map task is {} ({} / {})",
      viewKeysPerMapTask,viewKeys.length, importViewArgs.getNumMappers());

    CouchbaseViewInputSplit inputSplit = new CouchbaseViewInputSplit();
    int keysInCurrentSplit = 0;

    for (String viewKey : viewKeys) {
      //If we have enough keys, add the split and create a new one
      if(keysInCurrentSplit == viewKeysPerMapTask) {
        inputSplits.add(inputSplit);
        inputSplit = new CouchbaseViewInputSplit();
        keysInCurrentSplit = 0;
      }
      inputSplit.addKey(viewKey);
      keysInCurrentSplit++;
    }

    //Be sure to add any leftover keys
    if(keysInCurrentSplit>0){
      inputSplits.add(inputSplit);
    }

    return inputSplits;
  }

  @Override
  public RecordReader<Text, ViewRow> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    return new CouchbaseViewRecordReader();
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

}
