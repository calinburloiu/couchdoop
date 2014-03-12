package com.avira.bdo.chc.exp;

/**
 * This class should be used as a Hadoop value to represent a Couchbase operation, a key and a document to be stored. The
 * available operations are SET, ADD, REPLACE, APPEND, PREPEND and DELETE. For the DELETE operation the document is
 * ignored.
 */
public class CouchbaseAction {

  private CouchbaseOperation operation;
  private Object value;

  public CouchbaseAction(CouchbaseOperation operation, Object value) {
    this.operation = operation;
    this.value = value;
  }

  public static CouchbaseAction createSetAction(Object value) {
    return new CouchbaseAction(CouchbaseOperation.SET, value);
  }

  public static CouchbaseAction createAddAction(Object value) {
    return new CouchbaseAction(CouchbaseOperation.ADD, value);
  }

  public static CouchbaseAction createReplaceAction(Object value) {
    return new CouchbaseAction(CouchbaseOperation.REPLACE, value);
  }

  public static CouchbaseAction createAppendAction(Object value) {
    return new CouchbaseAction(CouchbaseOperation.APPEND, value);
  }

  public static CouchbaseAction createPrependAction(Object value) {
    return new CouchbaseAction(CouchbaseOperation.PREPEND, value);
  }

  public static CouchbaseAction createDeleteAction() {
    return new CouchbaseAction(CouchbaseOperation.DELETE, null);
  }

  public static CouchbaseAction createNoneAction() {
    return new CouchbaseAction(CouchbaseOperation.NONE, null);
  }

  public static CouchbaseAction createExistsAction() {
    return new CouchbaseAction(CouchbaseOperation.EXISTS, null);
  }

  public CouchbaseOperation getOperation() {
    return operation;
  }

  public void setOperation(CouchbaseOperation operation) {
    this.operation = operation;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "CouchbaseAction{" +
      "operation=" + operation +
      ", value=" + value +
      '}';
  }
}
