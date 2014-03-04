package com.avira.bdo.chc;

import org.apache.hadoop.conf.Configuration;

/**
 * Contract for JavaBeans which hold information read from the properties of a Hadoop
 * {@link org.apache.hadoop.conf.Configuration} file. This configuration in turn can be passed directly or may be read
 * from command line with Apache commons-cli library.
 */
public abstract class Args {

  private Configuration hadoopConfiguration;

  protected Args(Configuration hadoopConfiguration) {
    this.hadoopConfiguration = hadoopConfiguration;

    loadHadoopConfiguration(hadoopConfiguration);
  }

  public void loadHadoopConfiguration(Configuration hadoopConfiguration) {
    this.hadoopConfiguration = hadoopConfiguration;
  }

  public Configuration getHadoopConfiguration() {
    return hadoopConfiguration;
  }
}
