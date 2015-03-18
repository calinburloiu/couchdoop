/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.avira.couchdoop.exp;

import java.io.Serializable;

/**
 * This class should be used as a Hadoop value to represent a Couchbase operation, a key and a document to be stored. The
 * available operations are SET, ADD, REPLACE, APPEND, PREPEND and DELETE. For the DELETE operation the document is
 * ignored.
 */
public class CouchbaseAction implements Serializable {

  private CouchbaseOperation operation;
  private Object value;

  private int expiry = 0;

  public CouchbaseAction(CouchbaseOperation operation, Object value) {
    this.operation = operation;
    this.value = value;
  }

  public CouchbaseAction(CouchbaseOperation operation, Object value, int expiry) {
    this.operation = operation;
    this.value = value;
    this.expiry = expiry;
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

  /**
   * @deprecated Use {@link com.avira.couchdoop.exp.CouchbaseAction#createTouchAction()}
   */
  @Deprecated
  public static CouchbaseAction createExistsAction() {
    return new CouchbaseAction(CouchbaseOperation.TOUCH, null);
  }

  public static CouchbaseAction createTouchAction() {
    return new CouchbaseAction(CouchbaseOperation.TOUCH, null);
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

  public int getExpiry() {
    return expiry;
  }

  public void setExpiry(int expiry) {
    this.expiry = expiry;
  }

  @Override
  public String toString() {
    return "CouchbaseAction{" +
        "operation=" + operation +
        ", value=" + value +
        ", expiry=" + expiry +
        '}';
  }
}
