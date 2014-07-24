package com.avira.couchdoop.imp;

import com.avira.couchdoop.ArgsException;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;

/**
 * {@link com.avira.couchdoop.CouchbaseArgs} implementation which holds Couchbase view to HBase
 * import feature settings.
 */
public class ImportViewToHBaseArgs extends ImportViewArgs {

  private String table;
  private String columnFamily;
  private String columnQualifier;

  public ImportViewToHBaseArgs(Configuration hadoopConfiguration) throws ArgsException {
    super(hadoopConfiguration);
  }

  @Override
  protected Options getCliOptions() {
    Options options = super.getCliOptions();

    addOption(options, ARG_OUTPUT, true, true,
      "(required) HBase table name, column family and column qualifier separated by commas");

    return options;
  }

  @Override
  public void loadHadoopConfiguration() throws ArgsException {
    super.loadHadoopConfiguration();
    if (getOutput() == null) {
      return;
    }

    String[] splits = getOutput().split(",");
    if (splits.length != 3) {
      throw new ArgsException(
          "You must provide all of table and column family and column qualifier separated by commas as --output.");
    }
    table = splits[0];
    columnFamily = splits[1];
    columnQualifier = splits[2];
  }

  public String getTable() {
    return table;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public String getColumnQualifier() {
    return columnQualifier;
  }
}
