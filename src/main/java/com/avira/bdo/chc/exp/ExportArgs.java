package com.avira.bdo.chc.exp;

import com.avira.bdo.chc.ArgsException;
import com.avira.bdo.chc.CouchbaseArgs;
import com.couchbase.client.StoreType;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.omg.CORBA.ARG_IN;

/**
 * {@link com.avira.bdo.chc.CouchbaseArgs} implementation which holds Couchbase export feature settings.
 */
public class ExportArgs extends CouchbaseArgs {

  private String input;

  private StoreType storeType;

  public static final ArgDef ARG_INPUT = new ArgDef('i', "input");
  public static final ArgDef ARG_STORETYPE = new ArgDef('t', "couchbase.storeType");

  public ExportArgs(Configuration hadoopConfiguration) throws ArgsException {
    super(hadoopConfiguration);
  }

  public ExportArgs(Configuration hadoopConfiguration, String[] cliArgs) throws ArgsException {
    super(hadoopConfiguration, cliArgs);
  }

  @Override
  protected Options getCliOptions() {
    Options options = super.getCliOptions();

    addOption(options, ARG_INPUT, true, true,
        "(required) HDFS input directory");
    addOption(options, ARG_STORETYPE, true, false,
      "one of Couchbase store operations: SET, ADD, REPLACE, APPEND, PREPEND; defaults to SET");

    return options;
  }

  @Override
  public void loadFromHadoopConfiguration() throws ArgsException {
    super.loadFromHadoopConfiguration();

    input = hadoopConfiguration.get(ARG_INPUT.getPropertyName());
    storeType = getStoreType(hadoopConfiguration);
  }

  @Override
  protected void loadCliArgsIntoHadoopConfiguration(CommandLine cl) throws ArgsException {
    super.loadCliArgsIntoHadoopConfiguration(cl);

    setPropertyFromCliArg(cl, ARG_INPUT);
    setPropertyFromCliArg(cl, ARG_STORETYPE);
  }

  /**
   * @return HDFS input directory
   */
  public String getInput() {
    return input;
  }

  /**
   * Reads Couchbase store operation from the Hadoop configuration type.
   * @return Couchbase store operation to be used
   */
  public static StoreType getStoreType(Configuration hadoopConfiguration) throws ArgsException {
    String strStoreType = hadoopConfiguration.get(ARG_STORETYPE.getPropertyName());

    // Default value
    if (strStoreType == null) {
      return StoreType.SET;
    }

    try {
      return StoreType.valueOf(strStoreType);
    } catch (IllegalArgumentException e) {
      throw new ArgsException("Unrecognized store type '" + strStoreType +
        "'. Please provide one of the following: SET, ADD, REPLACE, APPEND, PREPEND.", e);
    }
  }

  /**
   * @return Couchbase store operation to be used
   */
  public StoreType getStoreType() {
    return storeType;
  }
}
