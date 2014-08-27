package com.avira.couchdoop.imp;

import com.avira.couchdoop.ArgsException;
import com.avira.couchdoop.CouchbaseArgs;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link com.avira.couchdoop.CouchbaseArgs} implementation which holds Couchbase view to HBase
 * import feature settings.
 */
public class ImportViewToHBaseArgs extends ImportViewArgs {

  private String table;

  private String columnFamily;

  private String columnQualifier;

  public static final ArgDef ARG_OUTPUT = new ArgDef('o', "output", true, true,
      "(required) HBase table name, column family and column qualifier separated by commas");

  public static final List<ArgDef> ARGS_LIST = new ArrayList<>(5);
  static {
    ARGS_LIST.add(ARG_OUTPUT);

    ARGS_LIST.add(ImportViewArgs.ARG_DESIGNDOC_NAME);
    ARGS_LIST.add(ImportViewArgs.ARG_VIEW_NAME);
    ARGS_LIST.add(ImportViewArgs.ARG_VIEW_KEYS);
    ARGS_LIST.add(ImportViewArgs.ARG_DOCS_PER_PAGE);

    ARGS_LIST.addAll(CouchbaseArgs.ARGS_LIST);
  }

  public ImportViewToHBaseArgs(Configuration conf) throws ArgsException {
    super(conf);
  }

  @Override
  public List<ArgDef> getArgsList(){
    return ImportViewToHBaseArgs.ARGS_LIST;
  }

  @Override
  public void loadFromHadoopConfiguration(Configuration conf) throws ArgsException {
    super.loadFromHadoopConfiguration(conf);
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
