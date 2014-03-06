package com.avira.bdo.chc;

import com.avira.bdo.chc.imp.CouchbaseViewImporter;
import com.avira.bdo.chc.imp.CouchbaseViewSerialImporter;

import java.util.Arrays;

/**
 * Main class for a tool which can be used to do basic data import from Couchbase to HDFS and basic data export to
 * Couchbase from HDFS.
 */
public class ImportExportTool {

  public static final String APP_NAME = "chc-tool";

  public static void main(String[] args) throws Exception{
    if (args.length < 1) {
      printUsage();
      System.exit(1);
    }

    String tool = args[0];
    String[] tailArgs = Arrays.copyOfRange(args, 1, args.length);
    if (tool.equals("import")) {
      CouchbaseViewImporter importer = new CouchbaseViewImporter();
      importer.start(tailArgs);
    } else if (tool.equals("serial-import")) {
      CouchbaseViewSerialImporter importer = new CouchbaseViewSerialImporter();
      importer.start(tailArgs);
    } else if (tool.equals("export")) {
      // TODO export
    } else {
      printUsage();
    }
  }

  public static void printUsage() {
    System.err.println("Usage:\n" +
      APP_NAME + " import [OPTIONS]\n" +
      APP_NAME + " serial-import [OPTIONS]\n" +
      APP_NAME + " export [OPTIONS]\n");
  }
}
