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
      importer.start(designDoc, viewName, viewKey, docsPerPage, destinationDir);
    } catch (IOException e) {
      LOGGER.error(ExceptionUtils.getStackTrace(e));
    }
  }

  public void start(String designDoc, String viewName, String viewKey, int docsPerPage, String destinationDir) throws IOException {
    LOGGER.info("Connecting to Couchbase...");
    List<URI> nodes = new ArrayList<URI>(3);
    nodes.add(URI.create(couchbaseUrl));
    try {
      couchbaseClient = new CouchbaseClient(nodes, couchbaseBucket, couchbasePassword);
      LOGGER.info("Connected to Couchbase.");
    } catch (IOException e) {
      LOGGER.error(ExceptionUtils.getStackTrace(e));
    }

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
}
