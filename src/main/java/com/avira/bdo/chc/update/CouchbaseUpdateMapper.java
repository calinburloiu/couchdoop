package com.avira.bdo.chc.update;

import com.avira.bdo.chc.ArgsException;
import com.avira.bdo.chc.exp.CouchbaseAction;
import com.avira.bdo.chc.exp.ExportArgs;
import com.couchbase.client.CouchbaseClient;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This Mapper class is meant to update Couchbase documents, by using data from a configurable InputFormat.
 *
 * <p>The Couchbase keys should be read from the configured InputFormat. Any other data read from that source
 * can be combined with current document values from Couchbase in order to compute the values.</p>
 *
 * <p>Extensions of this class should implement </p>
 */
public abstract class CouchbaseUpdateMapper<KEYIN, VALUEIN, T> extends Mapper<KEYIN, VALUEIN, String, CouchbaseAction> {

  protected CouchbaseClient couchbaseClient;

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseUpdateMapper.class);

  public static class HadoopInput<T> {
    private String couchbaseKey;
    private T hadoopData;

    public HadoopInput() {
    }

    public HadoopInput(String couchbaseKey, T hadoopData) {
      set(couchbaseKey, hadoopData);
    }

    public void set(String couchbaseKey, T hadoopData) {
      this.couchbaseKey = couchbaseKey;
      this.hadoopData = hadoopData;
    }

    public String getCouchbaseKey() {
      return couchbaseKey;
    }

    public void setCouchbaseKey(String couchbaseKey) {
      this.couchbaseKey = couchbaseKey;
    }

    public T getHadoopData() {
      return hadoopData;
    }

    public void setHadoopData(T hadoopData) {
      this.hadoopData = hadoopData;
    }
  }

  protected abstract HadoopInput<T> transform(KEYIN hKey, VALUEIN hValue);

  protected abstract CouchbaseAction merge(T t, Object cbInputValue);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    ExportArgs args;
    try {
      args = new ExportArgs(context.getConfiguration());
    } catch (ArgsException e) {
      throw new IllegalArgumentException(e);
    }

    LOGGER.info("Connecting to Couchbase...");
    couchbaseClient = new CouchbaseClient(args.getUrls(), args.getBucket(), args.getPassword());
    LOGGER.info("Connected to Couchbase.");
  }

  @Override
  protected void map(KEYIN hKey, VALUEIN hValue, Context context) throws IOException, InterruptedException {

  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    LOGGER.info("Disconnecting from Couchbase...");
    couchbaseClient.shutdown();
  }
}
