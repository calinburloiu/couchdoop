package com.avira.bdo.chc.update;

import com.avira.bdo.chc.exp.CouchbaseAction;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * {@link com.avira.bdo.chc.update.CouchbaseUpdateMapper} implementation which does the job serially without threads
 * and producer-consumer queue. It is not intended to be used in production. It is solely used as a reference in
 * benchmarking the threaded implementation.
 */
public abstract class CouchbaseSerialUpdateMapper<KEYIN, VALUEIN, T> extends CouchbaseUpdateMapper<KEYIN, VALUEIN, T> {

  @Override
  protected void map(KEYIN hKey, VALUEIN hValue, Context context) throws IOException, InterruptedException {
    // Transform the data received from the InputFormat.
    HadoopInput<T> hadoopInput = transform(hKey, hValue, context);
    if (hadoopInput == null) {
      return;
    }

    // Retrieve the current Couchbase document.
    Object couchbaseInputValue = couchbaseClient.get(hadoopInput.getCouchbaseKey());

    // Compute the Couchbase operation and the new output document.
    CouchbaseAction action = merge(hadoopInput.getHadoopData(), couchbaseInputValue, context);

    // Write the newly updated document back to Couchbase.
    context.write(hadoopInput.getCouchbaseKey(), action);
  }
}
