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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This mapper maps key-value pairs read from TSV files as documents in Couchbase by using keys as IDs and values as
 * documents.
 */
public class TsvToCouchbaseMapper extends Mapper<LongWritable, Text, String, CouchbaseAction> {

  private CouchbaseOperation operation;

  private static final String COUNTER_ERRORS = "ERRORS";

  enum Error { LINES_WITH_WRONG_COLUMNS_COUNT }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    try {
      operation = ExportArgs.getOperation(context.getConfiguration());
    } catch (ArgsException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String docId;
    CouchbaseAction action;
    String[] pair = value.toString().split("\t");

    if (operation.equals(CouchbaseOperation.DELETE)) {
      if (pair.length <= 1) {
        context.getCounter(Error.LINES_WITH_WRONG_COLUMNS_COUNT).increment(1);
        return;
      }

      docId = pair[0];
      action = CouchbaseAction.createDeleteAction();
    } else {
      // Skip error line.
      if (pair.length != 2) {
        context.getCounter(Error.LINES_WITH_WRONG_COLUMNS_COUNT).increment(1);
        return;
      }

      docId = pair[0];
      action = new CouchbaseAction(operation, pair[1]);
    }

    context.write(docId, action);
  }
}
