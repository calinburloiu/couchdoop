package com.avira.bdo.chc.imp;

import com.avira.bdo.chc.ArgsException;
import com.avira.bdo.chc.CouchbaseArgs;
import com.avira.bdo.chc.ImportExportTool;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * {@link com.avira.bdo.chc.Args} implementation which holds Couchbase view import tool settings.
 */
public class ImportViewArgs extends CouchbaseArgs {

  private String designDocumentName;

  private String viewName;

  private String[] viewKeys;

  private String output;

  private int documentsPerPage;
  private String docDelimiter;
  private String rowDelimiter;

  public static final ArgDef ARG_DESIGNDOC_NAME = new ArgDef('d', "couchbase.designDoc.name");
  public static final ArgDef ARG_VIEW_NAME = new ArgDef('v', "couchbase.view.name");
  public static final ArgDef ARG_VIEW_KEYS = new ArgDef('k', "couchbase.view.keys");
  public static final ArgDef ARG_OUTPUT = new ArgDef('o', "output");
  public static final ArgDef ARG_DOCS_PER_PAGE = new ArgDef('P', "couchbase.view.docsPerPage");

  public ImportViewArgs(Configuration hadoopConfiguration) {
    super(hadoopConfiguration);
  }

  public ImportViewArgs(Configuration hadoopConfiguration, String[] cliArgs) throws ArgsException {
    super(hadoopConfiguration, cliArgs);
  }

  @Override
  protected Options getCliOptions() {
    Options options = super.getCliOptions();

    addOption(options, ARG_DESIGNDOC_NAME, true, true,
        "(required) name of the design document");
    addOption(options, ARG_VIEW_NAME, true, true,
        "(required) name of the view");
    addOption(options, ARG_VIEW_KEYS, true, true,
        "(required) semicolon separated list of view keys (in JSON format) which are going to be distributed to mappers");
    addOption(options, ARG_OUTPUT, true, true,
        "(required) HDFS output directory");
    addOption(options, ARG_DOCS_PER_PAGE, true, false,
        "buffer of documents which are going to be retrieved at once at a mapper; defaults to 1024");

    return options;
  }

  @Override
  public void loadFromHadoopConfiguration() {
    super.loadFromHadoopConfiguration();

    designDocumentName = hadoopConfiguration.get(ARG_DESIGNDOC_NAME.getPropertyName());
    viewName = hadoopConfiguration.get(ARG_VIEW_NAME.getPropertyName());
    viewKeys = getViewKeys(hadoopConfiguration);
    output = hadoopConfiguration.get(ARG_OUTPUT.getPropertyName());

    documentsPerPage = hadoopConfiguration.getInt(ARG_DOCS_PER_PAGE.getPropertyName(), 1024);
  }

  @Override
  protected void loadCliArgsIntoHadoopConfiguration(CommandLine cl) {
    super.loadCliArgsIntoHadoopConfiguration(cl);

    setPropertyFromCliArg(cl, ARG_DESIGNDOC_NAME);
    setPropertyFromCliArg(cl, ARG_VIEW_NAME);
    setPropertyFromCliArg(cl, ARG_VIEW_KEYS);
    setPropertyFromCliArg(cl, ARG_OUTPUT);
    setPropertyFromCliArg(cl, ARG_DOCS_PER_PAGE);
  }

  public String getDesignDocumentName() {
    return designDocumentName;
  }

  public String getViewName() {
    return viewName;
  }

  public String[] getViewKeys() {
    return viewKeys;
  }

  public static String[] getViewKeys(Configuration hadoopConfiguration) {
    return StringUtils.split(hadoopConfiguration.get(ARG_VIEW_KEYS.getPropertyName()), '\\', ';');
  }

  public String getOutput() {
    return output;
  }

  public int getDocumentsPerPage() {
    return documentsPerPage;
  }

  public String getDocDelimiter() {
    return docDelimiter;
  }

  public String getRowDelimiter() {
    return rowDelimiter;
  }
}
