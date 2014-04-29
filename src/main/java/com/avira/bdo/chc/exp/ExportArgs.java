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

package com.avira.bdo.chc.exp;

import com.avira.bdo.chc.ArgsException;
import com.avira.bdo.chc.CouchbaseArgs;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;

/**
 * {@link com.avira.bdo.chc.CouchbaseArgs} implementation which holds Couchbase export feature settings.
 */
public class ExportArgs extends CouchbaseArgs {

  private String input;

  private CouchbaseOperation operation;

  public static final ArgDef ARG_INPUT = new ArgDef('i', "input");
  public static final ArgDef ARG_OPERATION = new ArgDef('t', "couchbase.operation");

  public ExportArgs(Configuration hadoopConfiguration) throws ArgsException {
    super(hadoopConfiguration);
  }

  public ExportArgs(Configuration hadoopConfiguration, String[] cliArgs) throws ArgsException {
    super(hadoopConfiguration, cliArgs);
  }

  @Override
  protected Options getCliOptions() {
    Options options = super.getCliOptions();

    addOption(options, ARG_INPUT, true, true,
        "(required) HDFS input directory");
    addOption(options, ARG_OPERATION, true, false,
      "one of Couchbase store operations: SET, ADD, REPLACE, APPEND, PREPEND, DELETE, EXISTS; defaults to SET");

    return options;
  }

  @Override
  public void loadFromHadoopConfiguration() throws ArgsException {
    super.loadFromHadoopConfiguration();

    input = hadoopConfiguration.get(ARG_INPUT.getPropertyName());
    operation = getOperation(hadoopConfiguration);
  }

  @Override
  protected void loadCliArgsIntoHadoopConfiguration(CommandLine cl) throws ArgsException {
    super.loadCliArgsIntoHadoopConfiguration(cl);

    setPropertyFromCliArg(cl, ARG_INPUT);
    setPropertyFromCliArg(cl, ARG_OPERATION);
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
}
