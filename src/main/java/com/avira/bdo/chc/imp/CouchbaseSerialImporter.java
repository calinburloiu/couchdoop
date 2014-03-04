package com.avira.bdo.chc.imp;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CouchbaseSerialImporter {

  private String couchbaseUrl;
  private String couchbaseBucket;
  private String couchbasePassword;
  private CouchbaseClient couchbaseClient;

  private static final String PAGE_FILE_BASENAME = "part";

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSerialImporter.class);

  public CouchbaseSerialImporter(String couchbaseUrl, String couchbaseBucket, String couchbasePassword) {
    this.couchbaseUrl = couchbaseUrl;
    this.couchbaseBucket = couchbaseBucket;
    this.couchbasePassword = couchbasePassword;
  }

  public static void main(String[] args) {
    String couchbaseUrl;
    String couchbaseBucket;
    String couchbasePassword;
    String designDoc;
    String viewName;
    String viewKey;
    int docsPerPage = 1024;
    String destinationDir = "";

    // Check arguments.
    if (args.length >= 7) {
      couchbaseUrl = args[0];
      couchbaseBucket = args[1];
      couchbasePassword = args[2];
      designDoc = args[3];
      viewName = args[4];
      viewKey = args[5];
      docsPerPage = Integer.parseInt(args[6]);
      destinationDir = args[7];
    } else {
      LOGGER.error(
        "Usage: $EXE <couchbase_url> <couchbase_bucket> <couchbase_password> <design_document> <view_name> <view_key> <docs_per_page> <destination_dir>");
      return;
    }

    CouchbaseSerialImporter importer = new CouchbaseSerialImporter(couchbaseUrl, couchbaseBucket, couchbasePassword);
    try {
//      importer.start(designDoc, viewName, viewKey, docsPerPage, destinationDir);
      List<String> splits = importer.printSplits(designDoc, viewName, viewKey, 10);
      LOGGER.info("___________________________________");
      importer.printRowByUsingSplits(designDoc, viewName, viewKey, 10, splits);
    } catch (IOException e) {
      LOGGER.error(ExceptionUtils.getStackTrace(e));
    }
  }

  public void printRowByUsingSplits(String designDoc, String viewName, String viewKey, int splitSize, List<String> splits)
      throws IOException {
    connectToCouchbase();

    View view = couchbaseClient.getView(designDoc, viewName);
    ViewResponse response;
    int count = 0;

    for (int i = 0; i < splits.size(); i++) {
      Query query = new Query();
      query.setKey(viewKey);
      query.setIncludeDocs(false);
      query.setStartkeyDocID(splits.get(i));
      query.setLimit(splitSize);

      response = couchbaseClient.query(view, query);
      for (ViewRow row : response) {
        LOGGER.info(row.getId());
        count++;
      }
    }

    LOGGER.info("Disconnecting from Couchbase...");
    couchbaseClient.shutdown();

    LOGGER.info("Total number of row is " + count + ".");
  }

  public List<String> printSplits(String designDoc, String viewName, String viewKey, int splitSize) throws IOException {
    connectToCouchbase();

    View view = couchbaseClient.getView(designDoc, viewName);
    ViewResponse response;
    Iterator<ViewRow> rowIt;

    String startkeyDocId = "\0";
    ArrayList<String> splits = new ArrayList<String>();
    splits.add(startkeyDocId);
    LOGGER.info("Split start key: " + startkeyDocId);

    while (true) {
      Query query = new Query();
      query.setKey(viewKey);
      query.setIncludeDocs(false);
      query.setStartkeyDocID(startkeyDocId);
      query.setSkip(splitSize);
      query.setLimit(1);

      response = couchbaseClient.query(view, query);
      rowIt = response.iterator();
      if (rowIt.hasNext()) {
        startkeyDocId = rowIt.next().getId();
        splits.add(startkeyDocId);
        LOGGER.info("Split start key: " + startkeyDocId);
      } else {
        break;
      }
    }

    LOGGER.info("Disconnecting from Couchbase...");
    couchbaseClient.shutdown();

    return splits;
  }

  public void start(String designDoc, String viewName, String viewKey, int docsPerPage, String destinationDir)
      throws IOException {
    connectToCouchbase();

    // Create the Hadoop configuration object.
    Configuration conf = new Configuration();

    View view = couchbaseClient.getView(designDoc, viewName);
    Query query = new Query();
    query.setKey(viewKey);
    query.setIncludeDocs(true);

    Paginator pages = couchbaseClient.paginatedQuery(view, query, docsPerPage);
    ViewResponse response;
    int pageNo = 0;
    PageFileWriter writer = null;
    try {
      while (pages.hasNext()) {
        LOGGER.info("Writing page " + pageNo + "...");

        // Get page rows.
        response = pages.next();

        // Prepare the object which writes the page to a file.
        writer = new PageFileWriter(conf, destinationDir, PAGE_FILE_BASENAME, pageNo);

        // Iterate on each row.
        for (ViewRow row : response) {
          String key = row.getId();
          String doc = row.getDocument().toString();

          LOGGER.debug("Writing document with ID " + row.getId() + "...");
          writer.write(key, doc);
        }

        // Prepare for the next page.
        writer.close();
        pageNo++;
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    LOGGER.info("Disconnecting from Couchbase...");
    couchbaseClient.shutdown();
  }

  protected void connectToCouchbase() throws IOException {
    LOGGER.info("Connecting to Couchbase...");
    List<URI> nodes = new ArrayList<URI>(3);
    nodes.add(URI.create(couchbaseUrl));
    try {
      couchbaseClient = new CouchbaseClient(nodes, couchbaseBucket, couchbasePassword);
      LOGGER.info("Connected to Couchbase.");
    } catch (IOException e) {
      LOGGER.error(ExceptionUtils.getStackTrace(e));
      throw e;
    }
  }
}
