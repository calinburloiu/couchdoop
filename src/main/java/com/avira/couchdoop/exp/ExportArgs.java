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

import com.avira.couchdoop.ArgsException;
import com.avira.couchdoop.ArgsHelper;
import com.avira.couchdoop.CouchbaseArgs;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link com.avira.couchdoop.CouchbaseArgs} implementation which holds Couchbase export feature settings.
 */
public class ExportArgs extends CouchbaseArgs {

  private String input;

  private CouchbaseOperation operation;

  private int expiry;

  private String fieldsDelimiter;

  public static final ArgDef ARG_INPUT = new ArgDef('i', "input", true, true,
      "(required) HDFS input directory");
  public static final ArgDef ARG_OPERATION = new ArgDef('t', "couchbase.operation", true, false,
      "one of Couchbase store operations: SET, ADD, REPLACE, APPEND, PREPEND, DELETE, EXISTS; defaults to SET");
  public static final ArgDef ARG_EXPIRY = new ArgDef('x', "couchbase.expiry", true, false,
      "Couchbase document expiry value; defaults to 0 (doesn't expire)");
  public static final ArgDef ARG_DELIMITER_FIELDS = new ArgDef('d', "delimiter.fields", true, false,
      "Fields delimiter for the CSV input; defaults to tab");

  public static final List<ArgDef> ARGS_LIST = new ArrayList<>(4);
  static {
    ARGS_LIST.add(ARG_INPUT);
    ARGS_LIST.add(ARG_OPERATION);
    ARGS_LIST.add(ARG_EXPIRY);
    ARGS_LIST.add(ARG_DELIMITER_FIELDS);

    ARGS_LIST.addAll(CouchbaseArgs.ARGS_LIST);
  }

  public ExportArgs(Configuration conf) throws ArgsException {
    super(conf);
  }

  @Override
  public List<ArgDef> getArgsList(){
    return ExportArgs.ARGS_LIST;
  }

  @Override
  public void loadFromHadoopConfiguration(Configuration conf) throws ArgsException {
    super.loadFromHadoopConfiguration(conf);

    input = conf.get(ARG_INPUT.getPropertyName());
    operation = getOperation(conf);
    expiry = getExpiry(conf);
    fieldsDelimiter = conf.get(ARG_DELIMITER_FIELDS.getPropertyName(), "\t");
  }

  /**
   * @return HDFS input directory
   */
  public String getInput() {
    return input;
  }

  /**
   * Reads Couchbase store operation from the Hadoop configuration type.
   * @return Couchbase store operation to be used
   */
  public static CouchbaseOperation getOperation(Configuration hadoopConfiguration) throws ArgsException {
    String strCouchbaseOperation = hadoopConfiguration.get(ARG_OPERATION.getPropertyName());

    // Default value
    if (strCouchbaseOperation == null) {
      return CouchbaseOperation.SET;
    }

    try {
      return CouchbaseOperation.valueOf(strCouchbaseOperation);
    } catch (IllegalArgumentException e) {
      throw new ArgsException("Unrecognized store type '" + strCouchbaseOperation +
        "'. Please provide one of the following: SET, ADD, REPLACE, APPEND, PREPEND and DELETE.", e);
    }
  }

  /**
   * @return Couchbase store operation to be used
   */
  public CouchbaseOperation getOperation() {
    return operation;
  }

  public static int getExpiry(Configuration hadoopConfiguration) throws ArgsException {
    String strExpiry = hadoopConfiguration.get(ARG_EXPIRY.getPropertyName());

    // Default value
    if (strExpiry == null) {
      return 0;
    }

    try {
      return Integer.parseInt(strExpiry);
    } catch (NumberFormatException e) {
      throw new ArgsException("Unrecognized expiry value '" + strExpiry +
        "'. Please provide a positive integer.", e);
    }
  }

  public int getExpiry() {
    return expiry;
  }

  public String getFieldsDelimiter() {
    return fieldsDelimiter;
  }
}
