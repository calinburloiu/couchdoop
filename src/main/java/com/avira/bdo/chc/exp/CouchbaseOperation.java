package com.avira.bdo.chc.exp;

/**
 * Operation type which can be performed by Couchbase.
 */
public enum CouchbaseOperation {
  SET,
  ADD,
  REPLACE,
  APPEND,
  PREPEND,

  DELETE
}
