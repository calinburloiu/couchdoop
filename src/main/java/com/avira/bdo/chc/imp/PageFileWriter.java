package com.avira.bdo.chc.imp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;

/**
 * An instance of this class is responsible of writing a page read from Couchbase in an HDFS file by using Hadoop HDFS
 * API.
 */
public class PageFileWriter implements Closeable {

  private OutputStream outputStream;

  private String keyDocumentDelimiter = "\0";
  private String rowDelimiter = "\n";

  private static final Logger LOGGER = LoggerFactory.getLogger(PageFileWriter.class);

  public PageFileWriter(Configuration conf, String dirName, String baseName, int page) throws IOException {
    String destinationPath;
    if (dirName.isEmpty()) {
      // If no dirName is provided write the file in current directory.
      destinationPath = baseName + "-" + String.format("%05d", page);
    } else {
      destinationPath = dirName + "/" + baseName + "-" + String.format("%05d", page);
    }

    // Get the file system.
    FileSystem fileSystem = FileSystem.get(URI.create(destinationPath), conf);

    // Get an OutputStream for the output file.
    Path path = new Path(destinationPath);
    LOGGER.info("Creating file '" + path + "'...");
    outputStream = fileSystem.create(path);
  }

  /**
   * Write a Couchbase document
   * @param key Couchbase document ID
   * @param document Couchbase document value
   * @throws IOException
   */
  public void write(String key, String document) throws IOException {
    try {
      // Write the key.
      outputStream.write(key.getBytes("UTF-8"));

      // Write the key-document keyDocumentDelimiter.
      outputStream.write(keyDocumentDelimiter.getBytes("UTF-8"));

      // Write the document.
      outputStream.write(document.getBytes("UTF-8"));

      // Write the row keyDocumentDelimiter.
      outputStream.write(rowDelimiter.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) { LOGGER.error("Shouldn't happen."); }
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeStream(outputStream);
  }

  public String getKeyDocumentDelimiter() {
    return keyDocumentDelimiter;
  }

  public void setKeyDocumentDelimiter(String keyDocumentDelimiter) {
    this.keyDocumentDelimiter = keyDocumentDelimiter;
  }

  public String getRowDelimiter() {
    return rowDelimiter;
  }

  public void setRowDelimiter(String rowDelimiter) {
    this.rowDelimiter = rowDelimiter;
  }
}
