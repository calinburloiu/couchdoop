package com.avira.bdo.chc.imp;

import com.avira.bdo.chc.Args;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.util.Arrays;

/**
 * {@link com.avira.bdo.chc.Args} implementation which holds Couchbase view import tool settings.
 */
public class ImportViewArgs extends Args {

  private String designDocumentName;

  private String viewName;

  private String[] viewKeys;

  private int documentsPerPage;

  public static final String PROPERTY_DESIGN_DOC_NAME = "couchbase.designDoc.name";
  public static final String PROPERTY_VIEW_NAME = "couchbase.view.name";
  public static final String PROPERTY_VIEW_KEYS = "couchbase.view.keys";
  public static final String PROPERTY_DOCS_PER_PAGE = "couchbase.view.docsPerPage";

  public ImportViewArgs(Configuration hadoopConfiguration) {
    super(hadoopConfiguration);
  }

  @Override
  public void loadHadoopConfiguration(Configuration hadoopConfiguration) {
    super.loadHadoopConfiguration(hadoopConfiguration);

    designDocumentName = hadoopConfiguration.get(PROPERTY_DESIGN_DOC_NAME);
    viewName = hadoopConfiguration.get(PROPERTY_VIEW_NAME);
    viewKeys = getViewKeys(hadoopConfiguration);

    documentsPerPage = hadoopConfiguration.getInt(PROPERTY_DOCS_PER_PAGE, 1024);
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
    return StringUtils.split(hadoopConfiguration.get(PROPERTY_VIEW_KEYS), '\\', ';');
  }

  public int getDocumentsPerPage() {
    return documentsPerPage;
  }

  @Override
  public String toString() {
    return "ImportViewArgs{" +
      "designDocumentName='" + designDocumentName + '\'' +
      ", viewName='" + viewName + '\'' +
      ", viewKeys=" + Arrays.toString(viewKeys) +
      ", documentsPerPage=" + documentsPerPage +
      '}';
  }
}
