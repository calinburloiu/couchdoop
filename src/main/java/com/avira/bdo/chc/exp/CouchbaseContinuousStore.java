package com.avira.bdo.chc.exp;

import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;

/**
 * Callback SAM function which is called when a store operation to Couchbase is completed. The status is checked and
 * if the operation failed it is going to be retried again and again, growing the retry interval exponentially until
 * a threshold is reached. continuous
 */
public class CouchbaseContinuousStore implements OperationCompletionListener {

  private CouchbaseClient couchbaseClient;
  private CouchbaseOperation operation;
  private String key;
  private Object value;
  private int backoffExp;
  private int maxTries;

  private boolean done = false;
  private boolean result = false;

  protected CouchbaseContinuousStore(CouchbaseClient couchbaseClient, CouchbaseOperation operation,
                                  String key, Object value, int backoffExp, int maxTries) {
    this.couchbaseClient = couchbaseClient;
    this.operation = operation;
    this.key = key;
    this.value = value;
    this.backoffExp = backoffExp;
    this.maxTries = maxTries;
  }

  public static void start(CouchbaseClient couchbaseClient, CouchbaseOperation operation,
                                  String key, Object value, int maxTries) {
    CouchbaseContinuousStore ccs = new CouchbaseContinuousStore(couchbaseClient, operation, key, value, 0, maxTries);
    ccs.store().addListener(ccs);
  }

  protected OperationFuture<Boolean> store() {
    switch (operation) {
      case SET:
        return couchbaseClient.set(key, value);
      case ADD:
        return couchbaseClient.add(key, value);
      case REPLACE:
        return couchbaseClient.replace(key, value);
      case APPEND:
        return couchbaseClient.append(key, value);
      case PREPEND:
        return couchbaseClient.prepend(key, value);
      case DELETE:
        return couchbaseClient.delete(key);
      case EXISTS:
        return couchbaseClient.touch(key, 0);
      default:
        // Ignore this action.
        return null;
    }
  }

  @Override
  public void onComplete(OperationFuture<?> future) throws Exception {
    if (!future.getStatus().isSuccess()) {
      if (backoffExp <= maxTries) {
        int retryInterval = 10 * (int) Math.exp(backoffExp);
        Thread.sleep(retryInterval);
        backoffExp++;

        store().addListener(this);
      } else {
        result = false;
        done = true;
      }
    } else {
      result = (Boolean) future.get();
      done = true;
    }
  }

  public boolean getResult() {
    if (!isDone()) {
      throw new IllegalStateException("Operation is not done yet.");
    }

    return result;
  }

  public boolean isDone() {
    return done;
  }
}
