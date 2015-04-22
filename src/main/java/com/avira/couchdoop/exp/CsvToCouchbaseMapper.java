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
import java.util.HashSet;
import java.util.Set;

/**
 * This mapper maps key-value pairs read from TSV files as documents in Couchbase by using keys as IDs and values as
 * documents.
 */
public class CsvToCouchbaseMapper extends Mapper<LongWritable, Text, String, CouchbaseAction> {

  private CouchbaseOperation operation;
  private int expiry;
  private String fieldsDelimiter;

  private static final Set<CouchbaseOperation> UNARY_OPERATIONS = new HashSet<CouchbaseOperation>(){{
    add(CouchbaseOperation.DELETE);
    add(CouchbaseOperation.TOUCH);
  }};

  private static final String COUNTER_ERRORS = "ERRORS";

  enum Error { LINES_WITH_WRONG_COLUMNS_COUNT }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    try {
      ExportArgs exportArgs = new ExportArgs(context.getConfiguration());

      operation = exportArgs.getOperation();
      expiry = exportArgs.getExpiry();
      fieldsDelimiter = exportArgs.getFieldsDelimiter();
    } catch (ArgsException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Object doc;
    String docId;
    CouchbaseAction action;
    String[] pair = value.toString().split(fieldsDelimiter);

    // Validate number of columns against operation.
    if (UNARY_OPERATIONS.contains(operation) && pair.length < 1 ||
        !UNARY_OPERATIONS.contains(operation) && pair.length < 2) {
      context.getCounter(Error.LINES_WITH_WRONG_COLUMNS_COUNT).increment(1);
      return;
    }

    docId = pair[0];
    doc = (pair.length >= 2 ? pair[1] : null);
    action = new CouchbaseAction(operation, doc, expiry);

    context.write(docId, action);
  }
}
