package com.avira.bdo.chc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link com.avira.bdo.chc.Args} implementation which holds Couchbase connection settings.
 */
public class CouchbaseArgs extends Args {

  private List<URI> urls;
  private String bucket;
  private String password;

  public static final String PROPERTY_COUCHBASE_URLS = "couchbase.urls";
  public static final String PROPERTY_COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String PROPERTY_COUCHBASE_PASSWORD = "couchbase.password";

  public CouchbaseArgs(Configuration hadoopConfiguration) {
    super(hadoopConfiguration);
  }

  @Override
  public void loadHadoopConfiguration(Configuration hadoopConfiguration) {
    super.loadHadoopConfiguration(hadoopConfiguration);

    String[] urlStrings = StringUtils.split(hadoopConfiguration.get(PROPERTY_COUCHBASE_URLS));
    urls = new ArrayList<URI>();
    for (String urlString: urlStrings) {
      urls.add(URI.create(urlString));
    }

    bucket = hadoopConfiguration.get(PROPERTY_COUCHBASE_BUCKET);
    password = hadoopConfiguration.get(PROPERTY_COUCHBASE_PASSWORD);
  }

  public List<URI> getUrls() {
    return urls;
  }

  public String getBucket() {
    return bucket;
  }

  public String getPassword() {
    return password;
  }

  @Override
  public String toString() {
    return "CouchbaseArgs{" +
      "urls=" + urls +
      ", bucket='" + bucket + '\'' +
      ", password='" + password + '\'' +
      '}';
  }
}
